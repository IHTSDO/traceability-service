package org.ihtsdo.otf.traceabilityservice.service;

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import org.ihtsdo.otf.traceabilityservice.util.QueryHelper;
import org.ihtsdo.otf.traceabilityservice.domain.*;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.client.elc.NativeQueryBuilder;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

import static co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders.bool;

@Component
public class ActivityService {

	private final ElasticsearchOperations elasticsearchOperations;

	public ActivityService(ElasticsearchOperations elasticsearchOperations) {
		this.elasticsearchOperations = elasticsearchOperations;
	}

	/**
	 * @param request ActivitySearchRequest
	 * @param page PageRequest
	 * @return Activities matched and filtered depending on requests
	 */
	public Page<Activity> getActivities(ActivitySearchRequest request, Pageable page) {

		final BoolQuery.Builder query = bool();

		if (request.getOriginalBranch() != null && !request.getOriginalBranch().isEmpty()) {
			query.must(QueryHelper.termQuery(Activity.Fields.branch, request.getOriginalBranch()));
		}
		if (request.getOnBranch() != null && !request.getOnBranch().isEmpty()) {
			BoolQuery.Builder boolQuery = bool();
			// One of these conditions must be true.  Either
			boolQuery.should(QueryHelper.termQuery(Activity.Fields.branch, request.getOnBranch()))
					 // Or
					.should(QueryHelper.termQuery(Activity.Fields.highestPromotedBranch, request.getOnBranch()));
			//Are we also checking for activity that might have been promoted higher, elsewhere?
			if (request.isIncludeHigherPromotions()) {
				for (String higherBranch : BranchUtils.getAncestorBranches(request.getOnBranch())) {
					boolQuery.should(QueryHelper.termQuery(Activity.Fields.highestPromotedBranch, higherBranch));
				}
			}
			query.must(List.of(new Query(boolQuery.build())));
		}
		if (request.getSourceBranch() != null && !request.getSourceBranch().isEmpty()) {
			query.must(QueryHelper.termQuery(Activity.Fields.sourceBranch, request.getSourceBranch()));
		}
		if (request.getBranchPrefix() != null && !request.getBranchPrefix().isEmpty()) {
			query.must(QueryHelper.prefixQuery(Activity.Fields.branch, request.getBranchPrefix()));
		}
		if (request.getActivityType() != null) {
			query.must(QueryHelper.termQuery(Activity.Fields.activityType, request.getActivityType().name()));
		}
		if (request.getConceptId() != null) {
			query.must(QueryHelper.termQuery(Activity.Fields.conceptChangesConceptId, request.getConceptId()));
		}
		if (request.getComponentId() != null) {
			query.must(QueryHelper.termQuery(Activity.Fields.componentChangesComponentId, request.getComponentId()));
		}
		if (request.getCommitDate() != null) {
			query.must(QueryHelper.termQuery(Activity.Fields.commitDate, request.getCommitDate().getTime()));
		}

		if (request.getFromDate() != null || request.getToDate() != null) {
			RangeQuery.Builder rangeQuery = QueryHelper.rangeQueryBuilder(Activity.Fields.commitDate);
			if (request.getFromDate() != null) {
				QueryHelper.withFrom(rangeQuery, request.getFromDate().getTime());
			}

			if (request.getToDate() != null) {
				QueryHelper.withTo(rangeQuery, request.getToDate().getTime());
			}

			query.must(QueryHelper.toQuery(rangeQuery));
		}
		
		if (request.isIntOnly()) {
			query.mustNot(QueryHelper.regexQuery(Activity.Fields.branch, ".*SNOMEDCT-.*"));
		}

		NativeQueryBuilder queryBuilder = new NativeQueryBuilder().withQuery(QueryHelper.toQuery(query));
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
		final BoolQuery.Builder query = bool();

		if (conceptIds != null && !conceptIds.isEmpty()) {
			query.must(QueryHelper.termsQuery(Activity.Fields.conceptChangesConceptId, conceptIds));
		}
		if (activityType != null) {
			query.must(QueryHelper.termQuery(Activity.Fields.activityType, activityType.name()));
		}
		if (user != null && !user.isEmpty()) {
			query.must(QueryHelper.termQuery(Activity.Fields.username, user));
		}
		NativeQueryBuilder queryBuilder = new NativeQueryBuilder().withQuery(QueryHelper.toQuery(query));
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
