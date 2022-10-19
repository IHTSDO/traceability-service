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
	private static final Logger LOGGER = LoggerFactory.getLogger(ActivityService.class);

	private final ActivityRepository activityRepository;
	private final ElasticsearchOperations elasticsearchOperations;

	public ActivityService(ActivityRepository activityRepository, ElasticsearchOperations elasticsearchOperations) {
		this.activityRepository = activityRepository;
		this.elasticsearchOperations = elasticsearchOperations;
	}

	/**
	 * @param request ActivitySearchRequest
	 * @param page PageRequest
	 * @return Activities matched and filtered depending on requests
	 */
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
	 * @param conceptIds A list of concept ids to search
	 * @param activityType @see{{@link ActivityType}
	 * @param user user name
	 * @param summaryOnly Set it true to return summary information only without component changes.
	 * @param page PageRequest
	 * @return Matched activities
	 */
	public Page<Activity> findActivitiesBy(List<Long> conceptIds, ActivityType activityType, String user, boolean summaryOnly, Pageable page) {
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
		if (summaryOnly) {
			queryBuilder.withSourceFilter(new FetchSourceFilter(null, new String[]{Activity.Fields.conceptChangesComponentChanges}));
		}
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
}
