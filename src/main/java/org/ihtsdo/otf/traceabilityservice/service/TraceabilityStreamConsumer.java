package org.ihtsdo.otf.traceabilityservice.service;

import org.ihtsdo.otf.traceabilityservice.Application;
import org.ihtsdo.otf.traceabilityservice.domain.*;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.ihtsdo.otf.traceabilityservice.repository.BranchRepository;
import org.ihtsdo.otf.traceabilityservice.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.json.JsonJsonParser;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class TraceabilityStreamConsumer {

	@Autowired
	private ActivityRepository activityRepository;

	@Autowired
	private BranchRepository branchRepository;

	@Autowired
	private UserRepository userRepository;

	private static final Pattern BRANCH_MERGE_COMMIT_COMMENT_PATTERN = Pattern.compile("^(.*) performed merge of (MAIN[^ ]*) to (MAIN[^ ]*)$");

	final Logger logger = LoggerFactory.getLogger(getClass());

	@JmsListener(destination = "${platform.name}." + Application.TRACEABILITY_QUEUE_SUFFIX)
	@Transactional
	public void receiveMessage(String message) {
		final Map<String, Object> traceabilityEntry = new JsonJsonParser().parseMap(message);
		String username = (String) traceabilityEntry.get("userId");
		final String commitComment = (String) traceabilityEntry.get("commitComment");
		final String branchPath = (String) traceabilityEntry.get("branchPath");
		final Long commitTimestampMillis = (Long) traceabilityEntry.get("commitTimestamp");
		final Date commitTimestamp = new Date(commitTimestampMillis);


		ActivityType activityType = null;

		final Matcher matcher = BRANCH_MERGE_COMMIT_COMMENT_PATTERN.matcher(commitComment);
		Branch mergeSourceBranch = null;
		if (matcher.matches()) {
			username = matcher.group(1);
			String sourceBranchPath = matcher.group(2);
			final String destinationBranchPath = matcher.group(3);
			activityType = destinationBranchPath.contains(sourceBranchPath) ? ActivityType.REBASE : ActivityType.PROMOTION;
			mergeSourceBranch = getCreateBranch(sourceBranchPath);
		}
		Branch branch = getCreateBranch(branchPath);

		User user = userRepository.findByUsername(username);
		if (user == null) {
			user = userRepository.save(new User(username));
		}

		final Activity activity = new Activity(user, commitComment, branch, commitTimestamp);
		if (mergeSourceBranch != null) {
			activity.setMergeSourceBranch(mergeSourceBranch);
		}

		boolean manualChangeFound = false;
		Map<String, Map<String, Object>> conceptChanges = (Map<String, Map<String, Object>>) traceabilityEntry.get("changes");
		if (conceptChanges != null) {
			for (String conceptId : conceptChanges.keySet()) {
				final Map<String, Object> conceptChangeMap = conceptChanges.get(conceptId);
				final ConceptChange conceptChange = new ConceptChange(Long.parseLong(conceptId));

				Map<String, Object> conceptSnapshot = (Map<String, Object>) conceptChangeMap.get("concept");
				Map<String, ComponentSubType> relationshipCharacteristicTypes = null;
				Map<String, ComponentSubType> descriptionTypes = null;
				if (conceptSnapshot != null) {
					relationshipCharacteristicTypes = getRelationshipCharacteristicTypes(conceptSnapshot);
					descriptionTypes = getDescriptionTypes(conceptSnapshot);
				}
				List<Map<String, String>> componentChangeMaps = (List<Map<String, String>>) conceptChangeMap.get("changes");
				for (Map<String, String> componentChangeMap : componentChangeMaps) {
					final String componentId = componentChangeMap.get("componentId");
					final String componentTypeString = componentChangeMap.get("componentType");
					final ComponentType componentType = ComponentType.valueOf(componentTypeString.toUpperCase());
					ComponentSubType componentSubType = null;
					switch (componentType) {
						case DESCRIPTION:
							if (descriptionTypes != null) {
								componentSubType = descriptionTypes.get(componentId);
							}
							break;
						case RELATIONSHIP:
							if (relationshipCharacteristicTypes != null) {
								componentSubType = relationshipCharacteristicTypes.get(componentId);
							}
							break;
					}
					final String changeTypeString = componentChangeMap.get("type");
					final ComponentChangeType componentChangeType = ComponentChangeType.valueOf(changeTypeString);
					if (!manualChangeFound) {
						if (componentType != ComponentType.RELATIONSHIP) {
							manualChangeFound = true;
						} else {
							if (ComponentSubType.STATED_RELATIONSHIP == componentSubType) {
								manualChangeFound = true;
							}
						}
					}
					conceptChange.addComponentChange(new ComponentChange(componentId, componentChangeType, componentType, componentSubType));
				}
				activity.addConceptChange(conceptChange);
			}
		}
		if (activityType == null) {
			activityType = !manualChangeFound && commitComment.equals("Classified ontology.") ? ActivityType.CLASSIFICATION_SAVE : ActivityType.CONTENT_CHANGE;
		}
		activity.setActivityType(activityType);

		logger.debug("Saving activity {}", activity);
		activityRepository.save(activity);

		if (activityType == ActivityType.PROMOTION) {
			// Move activities on the source branch up to the parent
			activityRepository.setHighestPromotedBranchWhereBranchEquals(branch, mergeSourceBranch);
		}

		logger.info("Consumed activity <" + commitComment + ">");
	}

	private Branch getCreateBranch(String branchPath) {
		Branch branch = branchRepository.findByBranchPath(branchPath);
		if (branch == null) {
			branch = branchRepository.save(new Branch(branchPath));
		}
		return branch;
	}

	private Map<String, ComponentSubType> getRelationshipCharacteristicTypes(Map<String, Object> conceptSnapshot) {
		final Map<String, ComponentSubType> relationshipCharacteristicTypes = new HashMap<>();
		final List<Map<String, String>> relationships = (List<Map<String, String>>) conceptSnapshot.get("relationships");
		if (relationships != null) { // If a concept is inactive relationships are not required
			for (Map<String, String> relationship : relationships) {
				final String characteristicType = relationship.get("characteristicType");
				relationshipCharacteristicTypes.put(relationship.get("relationshipId"),
						characteristicType.equals("INFERRED_RELATIONSHIP") ? ComponentSubType.INFERRED_RELATIONSHIP : ComponentSubType.STATED_RELATIONSHIP);
			}
		}
		return relationshipCharacteristicTypes;
	}

	private Map<String, ComponentSubType> getDescriptionTypes(Map<String, Object> conceptSnapshot) {
		final Map<String, ComponentSubType> descriptionTypes = new HashMap<>();
		final List<Map<String, String>> descriptions = (List<Map<String, String>>) conceptSnapshot.get("descriptions");
		if (descriptions != null) {
			for (Map<String, String> description : descriptions) {
				final String descriptionType = description.get("type");
				descriptionTypes.put(description.get("descriptionId"),
						descriptionType.equals("FSN") ? ComponentSubType.FSN_DESCRIPTION : ComponentSubType.SYNONYM_DESCRIPTION);
			}
		}
		return descriptionTypes;
	}

}
