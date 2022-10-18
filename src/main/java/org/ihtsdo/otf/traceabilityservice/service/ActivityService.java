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

import static org.elasticsearch.index.query.QueryBuilders.*;

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

	public Page<Activity> getActivities(ActivitySearchRequest request, Pageable page) {

		final BoolQueryBuilder query = boolQuery();

		if (request.getOriginalBranch() != null && !request.getOriginalBranch().isEmpty()) {
			query.must(termQuery(Activity.Fields.branch, request.getOriginalBranch()));
		}
		if (request.getOnBranch() != null && !request.getOnBranch().isEmpty()) {
			query.must(boolQuery()// One of these conditions must be true:
					// Either
					.should(termQuery(Activity.Fields.branch, request.getOnBranch()))
					// Or
					.should(termQuery(Activity.Fields.highestPromotedBranch, request.getOnBranch())));
		}
		if (request.getSourceBranch() != null && !request.getSourceBranch().isEmpty()) {
			query.must(termQuery(Activity.Fields.sourceBranch, request.getSourceBranch()));
		}
		if (request.getBranchPrefix() != null && !request.getBranchPrefix().isEmpty()) {
			query.must(prefixQuery(Activity.Fields.branch, request.getBranchPrefix()));
		}
		if (request.getActivityType() != null) {
			query.must(termQuery(Activity.Fields.activityType, request.getActivityType()));
		}
		if (request.getConceptId() != null) {
			query.must(termQuery(Activity.Fields.conceptChangesConceptId, request.getConceptId()));
		}
		if (request.getComponentId() != null) {
			query.must(termQuery(Activity.Fields.componentChangesComponentId, request.getComponentId()));
		}
		if (request.getCommitDate() != null) {
			query.must(termQuery(Activity.Fields.commitDate, request.getCommitDate().getTime()));
		}
		
		if (request.getFromDate() != null || request.getToDate() != null) {
			RangeQueryBuilder rangeQuery = rangeQuery(Activity.Fields.commitDate);
			if (request.getFromDate() != null) {
				rangeQuery.from(request.getFromDate().getTime());
			}
			
			if (request.getToDate() != null) {
				rangeQuery.to(request.getToDate().getTime());
			}
			query.must(rangeQuery);
		}
		
		if (request.isIntOnly()) {
			query.mustNot(regexpQuery(Activity.Fields.branch, ".*SNOMEDCT-.*"));
		}

		NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder().withQuery(query);
		if (request.isSummaryOnly()) {
			queryBuilder.withSourceFilter(new FetchSourceFilter(null, new String[]{Activity.Fields.conceptChanges}));
		} else if (request.isBrief()) {
			queryBuilder.withSourceFilter(new FetchSourceFilter(null, new String[]{Activity.Fields.conceptChangesComponentChanges}));
		}

		final SearchHits<Activity> search = elasticsearchOperations.search(queryBuilder.withPageable(page).build(), Activity.class);

		Page<Activity> results = new PageImpl<>(search.stream().map(SearchHit::getContent).collect(Collectors.toList()), page, search.getTotalHits());
		if (!request.isBrief() && !request.isSummaryOnly()) {
			filterResultsBy(request.getConceptId(), request.getComponentId(), results);
		}
		return results;
	}


	/**
	 * To avoid blowing buffer limits retrieving documents with huge numbers of rows, we will recover
	 * just the top level detail on a per concept basis, and then use the fact that only one concept
	 * has been requested to allow us to repopulate the detail (conceptChanges)
	 */
	public Page<Activity> findSummaryBy(List<Long> conceptIds, ActivityType activityType, Pageable page) {
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

	private void filterResultsBy(Long conceptId, String componentId, Page<Activity> activities) {
		for (Activity activity : activities.getContent()) {
			if (conceptId != null) {
				Set<ConceptChange> relevantChanges = activity.getConceptChanges().stream()
						.filter(change -> conceptId.equals(Long.parseLong(change.getConceptId())))
						.collect(Collectors.toSet());
				activity.setConceptChanges(relevantChanges);
			}
			if (activity.getConceptChanges() != null && componentId != null) {
				Set<ConceptChange> conceptChanges = new HashSet<>();
				for (ConceptChange conceptChange : activity.getConceptChanges()) {
					Set<ComponentChange> componentChanges = conceptChange.getComponentChanges().stream()
							.filter(componentChange -> componentId.equals(componentChange.getComponentId()))
							.collect(Collectors.toSet());
					if (!componentChanges.isEmpty()) {
						conceptChange.setComponentChanges(componentChanges);
						conceptChanges.add(conceptChange);
					}
				}
				activity.setConceptChanges(conceptChanges);
			}
		}
	}

	public Page<Activity> findBriefInfoOnlyBy(List<Long> conceptIds, ActivityType activityType, String user, Pageable page) {
		final BoolQueryBuilder query = boolQuery();
		if (conceptIds != null && !conceptIds.isEmpty()) {
			query.must(termsQuery(Activity.Fields.conceptChangesConceptId, conceptIds));
		}
		if (activityType != null) {
			query.must(termQuery(Activity.Fields.activityType, activityType));
		}
		if (user != null && !user.isEmpty()) {
			query.must(termQuery(Activity.Fields.username, user));
		}
		NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder().withQuery(query);
		queryBuilder.withSourceFilter(new FetchSourceFilter(null, new String[]{Activity.Fields.conceptChangesComponentChanges}));

		final SearchHits<Activity> search = elasticsearchOperations.search(queryBuilder.withPageable(page).build(), Activity.class);

		Page<Activity> results = new PageImpl<>(search.stream().map(SearchHit::getContent).collect(Collectors.toList()), page, search.getTotalHits());

		// Filter out concept ids not relevant
		for (Activity activity : results.getContent()) {
			if (conceptIds != null) {
				Set<ConceptChange> relevantChanges = activity.getConceptChanges().stream()
						.filter(change -> conceptIds.contains(Long.parseLong(change.getConceptId())))
						.collect(Collectors.toSet());
				activity.setConceptChanges(relevantChanges);
			}
		}
		return results;
	}
}
