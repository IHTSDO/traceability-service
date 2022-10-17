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
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.ihtsdo.otf.traceabilityservice.domain.Activity.Fields.*;

@Service
public class V3Point5MigrationTool {

	@Autowired
	private ElasticsearchRestTemplate elasticsearchRestTemplate;

	@Autowired
	private ActivityRepository activityRepository;

	private static final int MAX_CONCEPT_CHANGES = 1000;

	private static final int MAX_COMPONENT_CHANGES = 10_000;

	private static final int SIZE_TO_CHECK_BY_COMPONENT = 30_000;

	private static final Logger LOGGER = LoggerFactory.getLogger(V3Point5MigrationTool.class);

	private static final boolean DRY_RUN = false;

	private static final boolean REPORT_ONLY = true;

	public void start() {
		Instant start = Instant.now();
		// Find large documents with changes over 1000 concepts
		LOGGER.info("Start fetching traceability activities with large changes greater than {}", SIZE_TO_CHECK_BY_COMPONENT);
		List<Activity> largeActivities = findLargeActivitiesByComponentChanges(SIZE_TO_CHECK_BY_COMPONENT).getContent();
		LOGGER.info("Found {} traceability activities with large changes", largeActivities.size());
		largeActivities.stream().forEach(System.out::println);

		if (REPORT_ONLY) {
			return;
		}
		// Split big documents into batches
		largeActivities.forEach(activity -> {
			splitIntoBatches(activity);
			// Delete the activity with large documents
			LOGGER.info("Deleting {} ", activity.getId());
			if (!DRY_RUN) {
				activityRepository.delete(activity);
			}
			LOGGER.info("Deletion completed");
		});
		Instant end = Instant.now();
		LOGGER.info("Time taken in seconds {}", Duration.between(start, end).toSeconds());
	}

	private void splitByComponentChanges(Activity activity) {
		// Split by component changes
		activity.getConceptChanges().forEach(conceptChange -> {
			for (List<ComponentChange> batches :  Iterables.partition(conceptChange.getComponentChanges(), MAX_COMPONENT_CHANGES)) {
				Activity split = new Activity(activity.getUsername(), activity.getBranch(), activity.getSourceBranch(), activity.getCommitDate(), activity.getActivityType());
				ConceptChange updatedChange = new ConceptChange(conceptChange.getConceptId());
				updatedChange.setComponentChanges(new HashSet<>(batches));
				split.addConceptChange(updatedChange);
				split.setPromotionDate(activity.getPromotionDate());
				split.setHighestPromotedBranch(activity.getHighestPromotedBranch());
				if (!DRY_RUN) {
					activityRepository.save(split);
				}
				LOGGER.info("Saved {}", split);
			}
		});
	}

	private void splitIntoBatches(Activity activity) {
		Activity originalActivity = activityRepository.findById(activity.getId()).get();
		if (originalActivity == null) {
			throw new IllegalArgumentException("No activity found with id " + activity.getId());
		}

		LOGGER.info("Activity fetched for splitting {}", originalActivity);
		int maxConceptChangesPerBatch = 0;
		// Split by concepts first
		if (originalActivity.getConceptChanges().size() > MAX_CONCEPT_CHANGES) {
			maxConceptChangesPerBatch = MAX_CONCEPT_CHANGES;
		} else {
			// Concept batch size = MAX_CONCEPT_CHANGES / (Total component changes/MAX_COMPONENT_CHANGES)
			int totalChanges = originalActivity.getConceptChanges().stream().collect(Collectors.summingInt(c -> c.getComponentChanges().size()));
			LOGGER.info("Total component changes {}", totalChanges);
			maxConceptChangesPerBatch = MAX_CONCEPT_CHANGES / (totalChanges/MAX_COMPONENT_CHANGES);
		}
		if (originalActivity.getConceptChanges().size() > maxConceptChangesPerBatch) {
			LOGGER.info("Splitting by concepts in batch of {}", maxConceptChangesPerBatch);
			splitByConcepts(originalActivity, maxConceptChangesPerBatch);
		} else {
			LOGGER.info("Splitting by components in batch of {}", MAX_COMPONENT_CHANGES);
			splitByComponentChanges(originalActivity);
		}
	}

	private void splitByConcepts(Activity originalActivity, int maxConceptChangesPerBatch) {
		for (List<ConceptChange> batch : Iterables.partition(originalActivity.getConceptChanges(), maxConceptChangesPerBatch)) {
			Activity split = new Activity(originalActivity.getUsername(), originalActivity.getBranch(), originalActivity.getSourceBranch(), originalActivity.getCommitDate(), originalActivity.getActivityType());
			split.setConceptChanges(new HashSet<>(batch));
			split.setPromotionDate(originalActivity.getPromotionDate());
			split.setHighestPromotedBranch(originalActivity.getHighestPromotedBranch());
			if (!DRY_RUN) {
				activityRepository.save(split);
			}
			LOGGER.info("Saved {}", split);
		}
	}

	private Page<Activity> findLargeActivitiesByConceptChanges(int maxConceptChanges) {
		RequestOptions.Builder requestOptions = constructRequestOptions();
		final BoolQueryBuilder query = boolQuery();
		String scriptString = "doc['conceptChanges.conceptId'].length > params['total']";
		Map<String, Object> params = new HashMap<>();
		params.put("total", maxConceptChanges);
		Script script = new Script(ScriptType.INLINE, "painless", scriptString, params);

		query.must(termsQuery(activityType, Arrays.asList(ActivityType.CONTENT_CHANGE, ActivityType.CLASSIFICATION_SAVE)))
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

		query.must(termsQuery(activityType, Arrays.asList(ActivityType.CONTENT_CHANGE, ActivityType.CLASSIFICATION_SAVE)))
				.filter(scriptQuery(script));
		// Add sorting by script
		Script sortScript = new Script("doc['conceptChanges.componentChanges.componentId'].length");
		ScriptSortBuilder scriptSortBuilder = new ScriptSortBuilder(sortScript, ScriptSortBuilder.ScriptSortType.NUMBER).order(SortOrder.DESC);

		Pageable page = PageRequest.of(0, 100);
		NativeSearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(query)
				.withFields("id", branch, activityType, highestPromotedBranch)
				.withSort(scriptSortBuilder)
				.withPageable(page).build();
		final SearchHits<Activity> search = elasticsearchRestTemplate.search(searchQuery, Activity.class, requestOptions.build());
		return new PageImpl<>(search.stream().map(SearchHit::getContent).collect(Collectors.toList()), page, search.getTotalHits());
	}
}
