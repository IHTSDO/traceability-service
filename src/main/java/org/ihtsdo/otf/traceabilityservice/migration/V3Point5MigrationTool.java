package org.ihtsdo.otf.traceabilityservice.migration;

import com.google.common.collect.Iterables;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.ihtsdo.otf.traceabilityservice.domain.Activity;
import org.ihtsdo.otf.traceabilityservice.domain.ActivityType;
import org.ihtsdo.otf.traceabilityservice.domain.ComponentChange;
import org.ihtsdo.otf.traceabilityservice.domain.ConceptChange;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.*;
import org.springframework.data.elasticsearch.core.*;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.*;

@Service
public class V3Point5MigrationTool {

	@Autowired
	private ElasticsearchRestTemplate elasticsearchRestTemplate;

	@Autowired
	private ActivityRepository activityRepository;

	private static final int MAX_CONCEPT_CHANGES = 1000;

	private static final int MAX_COMPONENT_CHANGES = 10000;

	private static final int SIZE_TO_CHECK_BY_CONCEPT = 2000;

	private static final Logger LOGGER = LoggerFactory.getLogger(V3Point5MigrationTool.class);

	public void start() {
		Instant start = Instant.now();
		// Find large documents with changes over 1000 concepts
		LOGGER.info("Start fetching traceability activities with large changes greater than {}", SIZE_TO_CHECK_BY_CONCEPT);
		List<Activity> largeActivities = findLargeActivitiesByConceptChanges(SIZE_TO_CHECK_BY_CONCEPT).getContent();
		LOGGER.info("Found {} traceability activities with large changes", largeActivities.size());

		// Split big documents into batch of 1000
		largeActivities.forEach(activity -> {
			LOGGER.info("Start splitting traceability activity {}", activity);
			splitIntoBatchesByConcept(activity, MAX_CONCEPT_CHANGES);
			// Delete the activity with large documents
			LOGGER.info("Deleting {} ", activity);
			activityRepository.delete(activity);
			LOGGER.info("Deletion completed");
		});
		Instant end = Instant.now();
		LOGGER.info("Time taken in seconds {}", Duration.between(start, end).toSeconds());
	}

	private void splitIntoBatchesByComponent(Activity activity, int maxComponentChanges) {
		// Split by component changes
		activity.getConceptChanges().forEach(conceptChange -> {
			for (List<ComponentChange> batches :  Iterables.partition(conceptChange.getComponentChanges(), maxComponentChanges)) {
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

	private void splitIntoBatchesByConcept(Activity activity, int maxConceptChanges) {
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
		}
	}

	private Page<Activity> findLargeActivitiesByConceptChanges(int maxConceptChanges) {
		RequestOptions.Builder requestOptions = constructRequestOptions();
		final BoolQueryBuilder query = boolQuery();
		String scriptString = "doc['conceptChanges.conceptId'].length > params['total']";
		Map<String, Object> params = new HashMap<>();
		params.put("total", maxConceptChanges);
		Script script = new Script(ScriptType.INLINE, "painless", scriptString, params);

		query.must(termsQuery(Activity.Fields.activityType, Arrays.asList(ActivityType.CONTENT_CHANGE, ActivityType.CLASSIFICATION_SAVE)))
				.filter(scriptQuery(script));
		// Add sorting by script
		Script sortScript = new Script("doc['conceptChanges.conceptId'].length");
		ScriptSortBuilder scriptSortBuilder = new ScriptSortBuilder(sortScript, ScriptSortBuilder.ScriptSortType.NUMBER).order(SortOrder.DESC);

		Pageable page = PageRequest.of(0, 1);
		final SearchHits<Activity> search = elasticsearchRestTemplate.search(new NativeSearchQueryBuilder().withQuery(query).withSort(scriptSortBuilder).withPageable(page).build(), Activity.class, requestOptions.build());
		return new PageImpl<>(search.stream().map(SearchHit::getContent).collect(Collectors.toList()), page, search.getTotalHits());
	}

	private RequestOptions.Builder constructRequestOptions() {
		RequestOptions.Builder requestOptions = RequestOptions.DEFAULT.toBuilder();
		requestOptions.setHttpAsyncResponseConsumerFactory(new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(2 * 104857600));
		return requestOptions;
	}

	private Page<Activity> findLargeActivitiesByComponentChanges(int maxComponentChanges) {
		RequestOptions.Builder requestOptions = constructRequestOptions();
		final BoolQueryBuilder query = boolQuery();
		String scriptString = "doc['conceptChanges.componentChanges.componentId'].length > params['total']";
		Map<String, Object> params = new HashMap<>();
		params.put("total", maxComponentChanges);
		Script script = new Script(ScriptType.INLINE, "painless", scriptString, params);

		query.must(termsQuery(Activity.Fields.activityType, Arrays.asList(ActivityType.CONTENT_CHANGE, ActivityType.CLASSIFICATION_SAVE)))
				.filter(scriptQuery(script));
		// Add sorting by script
		Script sortScript = new Script("doc['conceptChanges.componentChanges.componentId'].length");
		ScriptSortBuilder scriptSortBuilder = new ScriptSortBuilder(sortScript, ScriptSortBuilder.ScriptSortType.NUMBER).order(SortOrder.DESC);

		Pageable page = PageRequest.of(0, 1);
		final SearchHits<Activity> search = elasticsearchRestTemplate.search(new NativeSearchQueryBuilder().withQuery(query).withSort(scriptSortBuilder).withPageable(page).build(), Activity.class, requestOptions.build());
		return new PageImpl<>(search.stream().map(SearchHit::getContent).collect(Collectors.toList()), page, search.getTotalHits());
	}
}
