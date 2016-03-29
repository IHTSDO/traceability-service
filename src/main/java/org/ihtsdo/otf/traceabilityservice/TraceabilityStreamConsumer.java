package org.ihtsdo.otf.traceabilityservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.ihtsdo.otf.traceabilityservice.domain.*;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.ihtsdo.otf.traceabilityservice.repository.BranchRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.json.JsonJsonParser;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;
import java.util.Map;

@Component
public class TraceabilityStreamConsumer {

	@Autowired
	private ActivityRepository activityRepository;

	@Autowired
	private BranchRepository branchRepository;

	@JmsListener(destination = "traceability-stream")
	public void receiveMessage(String message) {

		ObjectMapper objectMapper = new ObjectMapper();
//		objectMapper.

		final Map<String, Object> traceabilityEntry = new JsonJsonParser().parseMap(message);
		final String userId = (String) traceabilityEntry.get("userId");
		final String commitComment = (String) traceabilityEntry.get("commitComment");
		final String branchPath = (String) traceabilityEntry.get("branchPath");
		final Long commitTimestampMillis = (Long) traceabilityEntry.get("commitTimestamp");
		final Date commitTimestamp = new Date(commitTimestampMillis);

		final ActivityType activityType = ActivityType.CONTENT_CHANGE;

		Branch branch = branchRepository.findByBranchPath(branchPath);
		if (branch == null) {
			branch = branchRepository.save(new Branch(branchPath));
		}

		final Activity activity = new Activity(userId, commitComment, branch, commitTimestamp, activityType);

		Map<String, Map<String, Object>> conceptChanges = (Map<String, Map<String, Object>>) traceabilityEntry.get("changes");
		if (conceptChanges != null) {
			for (String conceptId : conceptChanges.keySet()) {
				final Map<String, Object> conceptChangeMap = conceptChanges.get(conceptId);
				final ConceptChange conceptChange = new ConceptChange(Long.parseLong(conceptId));

				List<Map<String, String>> componentChangeMaps = (List<Map<String, String>>) conceptChangeMap.get("changes");
				for (Map<String, String> componentChangeMap : componentChangeMaps) {
					final String componentTypeString = componentChangeMap.get("componentType");
					final ComponentType componentType = ComponentType.valueOf(componentTypeString.toUpperCase());
					final String componentId = componentChangeMap.get("componentId");
					final String changeTypeString = componentChangeMap.get("type");
					final ComponentChangeType componentChangeType = ComponentChangeType.valueOf(changeTypeString);
					conceptChange.addComponentChange(new ComponentChange(componentType, componentId, componentChangeType));
				}
				activity.addConceptChange(conceptChange);
			}
		}

		activityRepository.save(activity);

		System.out.println("Received <" + commitComment + ">");
	}

}
