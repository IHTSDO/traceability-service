package org.ihtsdo.otf.traceabilityservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.ihtsdo.otf.traceabilityservice.configuration.ApplicationProperties;
import org.ihtsdo.otf.traceabilityservice.util.QueryHelper;
import org.ihtsdo.otf.traceabilityservice.domain.*;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.client.elc.NativeQueryBuilder;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHitsIterator;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders.bool;

@Component
public class TraceabilityStreamConsumer {

	@Autowired
	private ActivityRepository activityRepository;

	@Autowired
	private ElasticsearchOperations elasticsearchOperations;

	@Autowired
	private ObjectMapper objectMapper;

	private final Logger logger = LoggerFactory.getLogger(getClass());

	@JmsListener(destination = "${platform.name}." + ApplicationProperties.TRACEABILITY_QUEUE_SUFFIX)
	@Transactional
	public void receiveMessage(String message) throws JsonProcessingException {
		ActivityMessage activityMessage = objectMapper.readValue(message, ActivityMessage.class);
		final String branchPath = activityMessage.getBranchPath();
		final String username = activityMessage.getUserId();
		final Date commitTimestamp = new Date(activityMessage.getCommitTimestamp());

		final ActivityType activityType = activityMessage.getActivityType();
		final String mergeSourceBranch = activityMessage.getSourceBranch();
		final Activity activity = new Activity(username, branchPath, mergeSourceBranch, commitTimestamp, activityType);

		final List<ActivityMessage.ConceptActivity> changes = activityMessage.getChanges();
		if (changes != null) {
			for (ActivityMessage.ConceptActivity conceptActivity : changes) {
				final String conceptId = conceptActivity.getConceptId();

				final ConceptChange conceptChange = new ConceptChange(conceptId);
				for (ActivityMessage.ComponentChange componentChange : conceptActivity.getComponentChanges()) {
					conceptChange.addComponentChange(new ComponentChange(componentChange.getComponentId(), componentChange.getChangeType(),
							componentChange.getComponentType(), componentChange.getComponentSubType(), componentChange.isEffectiveTimeNull(), componentChange.isSuperseded()));
				}
				activity.addConceptChange(conceptChange);
			}
		}

		logger.debug("Saving activity {}", activity);
		activityRepository.save(activity);

		if (activityType == ActivityType.PROMOTION) {
			// Move activities on the source branch up to the parent
			final List<ActivityType> contentActivityTypes = Lists.newArrayList(ActivityType.CLASSIFICATION_SAVE, ActivityType.CONTENT_CHANGE, ActivityType.REBASE);

			List<Activity> toSave = new ArrayList<>();

			try (final SearchHitsIterator<Activity> stream = elasticsearchOperations.searchForStream(new NativeQueryBuilder().withQuery(QueryHelper.toQuery(bool()
							.must(QueryHelper.termQuery(Activity.Fields.HIGHEST_PROMOTED_BRANCH, mergeSourceBranch))
							.must(QueryHelper.termsQuery(Activity.Fields.ACTIVITY_TYPE, contentActivityTypes))))
					.withPageable(PageRequest.of(0, 1_000))
					.build(), Activity.class)) {
				stream.forEachRemaining(activitySearchHit -> {
					final Activity activityToUpdate = activitySearchHit.getContent();
					activityToUpdate.setHighestPromotedBranch(branchPath);
					activityToUpdate.setPromotionDate(commitTimestamp);
					toSave.add(activityToUpdate);
				});
			}
			if (!toSave.isEmpty()) {
				logger.debug("Updating highest promoted branch on {} existing activities.", toSave.size());
				toSave.forEach(activityRepository::save);// Saving one at a time to avoid AWS permissions issue when using "/_bulk" URI
			}
		}

		logger.info("Consumed activity on {} @ {}", branchPath, commitTimestamp.getTime());
	}

}
