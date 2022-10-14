package org.ihtsdo.otf.traceabilityservice.migration;

import com.google.common.collect.Iterables;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.ihtsdo.otf.traceabilityservice.domain.Activity;
import org.ihtsdo.otf.traceabilityservice.domain.ActivityType;
import org.ihtsdo.otf.traceabilityservice.domain.ComponentChange;
import org.ihtsdo.otf.traceabilityservice.domain.ConceptChange;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.*;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.*;

@Service
public class V3Point5MigrationTool {

	@Autowired
	private ElasticsearchOperations elasticsearchOperations;

	@Autowired
	private ElasticsearchRestTemplate elasticsearchRestTemplate;

	@Autowired
	private RestHighLevelClient restHighLevelClient;

	@Autowired
	private ActivityRepository activityRepository;

	private static final int MAX_CONCEPT_CHANGES = 1000;

	private static final int MAX_COMPONENT_CHANGES = 2000;

	private static final Logger LOGGER = LoggerFactory.getLogger(V3Point5MigrationTool.class);

	public void start() {
		// Find large documents with changes over 1000 concepts
		LOGGER.info("Start fetching traceability activities with large changes greater than {}", MAX_COMPONENT_CHANGES);
		List<Activity> largeActivities = findLargeActivities(MAX_COMPONENT_CHANGES).getContent();
		LOGGER.info("Found {} traceability activities with large changes", largeActivities.size());

		// Split big documents into batch of 1000
		largeActivities.forEach(activity -> {
			LOGGER.info("Start splitting traceability activity {}", activity);
			splitIntoBatches(activity, MAX_CONCEPT_CHANGES);

			// Delete the activity with large documents
			LOGGER.info("Deleting {} ", activity);
			activityRepository.delete(activity);
			LOGGER.info("Deletion completed");
		});

	}

	private void splitIntoBatches(Activity activity, int maxConceptChanges) {
		// Split by concepts
		if (activity.getConceptChanges().size() > maxConceptChanges) {
			for (List<ConceptChange> partition : Iterables.partition(activity.getConceptChanges(), maxConceptChanges)) {
				Activity split = new Activity(activity.getUsername(), activity.getBranch(), activity.getSourceBranch(), activity.getCommitDate(), activity.getActivityType());
				split.setConceptChanges(new HashSet<>(partition));
				split.setPromotionDate(activity.getPromotionDate());
				split.setHighestPromotedBranch(activity.getHighestPromotedBranch());
				activityRepository.save(split);
				LOGGER.info("Saved {}", split);
			}
		} else {
			// Split by component changes
			activity.getConceptChanges().forEach(conceptChange -> {
				for (List<ComponentChange> batches :  Iterables.partition(conceptChange.getComponentChanges(), maxConceptChanges)) {
					Activity split = new Activity(activity.getUsername(), activity.getBranch(), activity.getSourceBranch(), activity.getCommitDate(), activity.getActivityType());
					ConceptChange updatedChange = new ConceptChange(conceptChange.getConceptId());
					updatedChange.setComponentChanges(new HashSet<>(batches));
					split.addConceptChange(updatedChange);
					split.setPromotionDate(activity.getPromotionDate());
					split.setHighestPromotedBranch(activity.getHighestPromotedBranch());
					activityRepository.save(split);
					LOGGER.info("Saved {}", split);
				}
			});
		}
	}

	private Page<Activity> findLargeActivities(int maxComponentChanges) {

		RequestOptions.Builder requestOptions = RequestOptions.DEFAULT.toBuilder();
		requestOptions.setHttpAsyncResponseConsumerFactory(
				new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(2 * 104857600));

		final BoolQueryBuilder query = boolQuery();
		String scriptString = "doc['conceptChanges.componentChanges.componentId'].length > params['total']";
		Map<String, Object> params = new HashMap<>();
		params.put("total", maxComponentChanges);
		Script script = new Script(ScriptType.INLINE, "painless", scriptString, params);

		query.must(termsQuery(Activity.Fields.activityType, Arrays.asList(ActivityType.CONTENT_CHANGE, ActivityType.CLASSIFICATION_SAVE)))
				.filter(scriptQuery(script));

		Pageable page = PageRequest.of(0, 100);
		final SearchHits<Activity> search = elasticsearchRestTemplate.search(new NativeSearchQueryBuilder().withQuery(query).withPageable(page).build(), Activity.class, requestOptions.build());
		return new PageImpl<>(search.stream().map(SearchHit::getContent).collect(Collectors.toList()), page, search.getTotalHits());
	}
}
