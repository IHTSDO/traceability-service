package org.ihtsdo.otf.traceabilityservice.service;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.ihtsdo.otf.traceabilityservice.domain.*;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.regexpQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

@Component
public class ActivityService {
	public static final int MAX_SIZE_FOR_PER_CONCEPT_RETRIEVAL = 10;
	private static final Logger LOGGER = LoggerFactory.getLogger(ActivityService.class);

	private final ActivityRepository activityRepository;
	private final ElasticsearchOperations elasticsearchOperations;

	public ActivityService(ActivityRepository activityRepository, ElasticsearchOperations elasticsearchOperations) {
		this.activityRepository = activityRepository;
		this.elasticsearchOperations = elasticsearchOperations;
	}

	/**
	 * Return paged Activity matching query.
	 *
	 * @param conceptIds   Field to match in query.
	 * @param activityType Optional field to match in query.
	 * @param user         Optional field to match in query
	 * @param page         Page request.
	 * @return Paged Activity matching query.
	 */
	public Page<Activity> findBy(List<Long> conceptIds, ActivityType activityType, String user, Pageable page) {
		LOGGER.debug("Finding paged activities.");
		boolean noUser = user == null || user.isEmpty();
		boolean noActivityType = activityType == null;

		if (noUser && noActivityType) {
			LOGGER.debug("No user or activityType present. Finding by conceptIds only.");
			return activityRepository.findBy(conceptIds, page);
		}

		if (noUser) {
			LOGGER.debug("No user present. Finding by conceptIds and activityType only.");
			return activityRepository.findBy(conceptIds, activityType, page);
		}

		if (noActivityType) {
			LOGGER.debug("No activityType present. Finding by conceptIds and user only.");
			return activityRepository.findBy(conceptIds, user, page);
		}

		LOGGER.debug("Finding by conceptIds, activityType and user.");
		return activityRepository.findBy(conceptIds, activityType, user, page);
	}

	public Page<Activity> getActivities(String originalBranch, String onBranch, String sourceBranch, ActivityType activityType, Long conceptId, String componentId,
			Date commitDate, Date fromDate, Date toDate, boolean intOnly, Pageable page) {

		final BoolQueryBuilder query = boolQuery();

		if (originalBranch != null && !originalBranch.isEmpty()) {
			query.must(termQuery(Activity.Fields.branch, originalBranch));
		}
		if (onBranch != null && !onBranch.isEmpty()) {
			query.must(boolQuery()// One of these conditions must be true:
					// Either
					.should(termQuery(Activity.Fields.branch, onBranch))
					// Or
					.should(termQuery(Activity.Fields.highestPromotedBranch, onBranch)));
		}
		if (sourceBranch != null && !sourceBranch.isEmpty()) {
			query.must(termQuery(Activity.Fields.sourceBranch, sourceBranch));
		}
		if (activityType != null) {
			query.must(termQuery(Activity.Fields.activityType, activityType));
		}
		if (conceptId != null) {
			query.must(termQuery(Activity.Fields.conceptChangesConceptId, conceptId));
		}
		if (componentId != null) {
			query.must(termQuery(Activity.Fields.componentChangesComponentId, componentId));
		}
		if (commitDate != null) {
			query.must(termQuery(Activity.Fields.commitDate, commitDate.getTime()));
		}
		
		if (fromDate != null || toDate != null) {
			RangeQueryBuilder rangeQuery = rangeQuery(Activity.Fields.commitDate);
			if (fromDate != null) {
				rangeQuery.from(fromDate.getTime());
			}
			
			if (toDate != null) {
				rangeQuery.to(toDate.getTime());
			}
			query.must(rangeQuery);
		}
		
		if (intOnly == true) {
			query.mustNot(regexpQuery(Activity.Fields.branch, ".*SNOMEDCT-.*"));
		}

		final SearchHits<Activity> search = elasticsearchOperations.search(new NativeSearchQueryBuilder().withQuery(query).withPageable(page).build(), Activity.class);
		return new PageImpl<>(search.stream().map(SearchHit::getContent).collect(Collectors.toList()), page, search.getTotalHits());
	}


	/**
	 * To avoid blowing buffer limits retrieving documents with huge numbers of rows, we will recover
	 * just the top level detail on a per concept basis, and then use the fact that only one concept
	 * has been requested to allow us to repopulate the detail (conceptChanges)
	 */
	public Page<Activity> findSummaryBy(List<Long> conceptIds, ActivityType activityType, Pageable page) {
		//An RF2 import could create an activity with hundreds of thousands of concept changes.
		//TOOD Investigate re-doing the mapping using nester and inner_hits to only return the rows we're interested in
		//So we'll just recover the summary and populate the particular concept we're interested in for now
		Map<Integer, Activity> activitiesMap = new HashMap<>();
		for (Long conceptId : conceptIds) {
			NativeSearchQuery query = getActivityQuery(conceptId, activityType);
			SearchHits<Activity> results = elasticsearchOperations.search(query, Activity.class);
			results.get().forEach(hit -> {
				Activity activity = hit.getContent();
				//Have we seen this activity before?  Add this concept in any event
				if (activitiesMap.containsKey(activity.hashCode())) {
					activity = activitiesMap.get(activity.hashCode());
				}
				activity.getConceptChanges().add(new ConceptChange(conceptId.toString()));
				activitiesMap.put(activity.hashCode(), activity);
			});
		}
		List<Activity> activities = new ArrayList<>(activitiesMap.values());
		final int start = (int)page.getOffset();
		final int end = Math.min((start + page.getPageSize()), activities.size());
		return new PageImpl<>(activities.subList(start, end), page, activities.size());
	}

	private NativeSearchQuery getActivityQuery(Long conceptId, ActivityType activityType) {
		SourceFilter sourceFilter = new FetchSourceFilter(null, new String[]{Activity.Fields.conceptChanges});
		final BoolQueryBuilder clauses = boolQuery()
				.must(termQuery(Activity.Fields.conceptChangesConceptId, conceptId))
				.must(termQuery(Activity.Fields.activityType, activityType));

		return new NativeSearchQueryBuilder().withQuery(
				new BoolQueryBuilder().must(clauses))
				.withSourceFilter(sourceFilter)
				.build();
	}
}
