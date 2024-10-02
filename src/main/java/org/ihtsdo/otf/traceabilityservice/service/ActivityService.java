package org.ihtsdo.otf.traceabilityservice.service;

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.json.JsonData;
import com.google.common.base.Splitter;
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
		doBranchFiltering(query, request);
		doContentFiltering(query, request);

		NativeQueryBuilder queryBuilder = new NativeQueryBuilder().withQuery(QueryHelper.toQuery(query));
		if (request.isSummaryOnly()) {
			queryBuilder.withSourceFilter(new FetchSourceFilter(null, new String[]{Activity.Fields.CONCEPT_CHANGES}));
		} else if (request.isBrief()) {
			queryBuilder.withSourceFilter(new FetchSourceFilter(null, new String[]{Activity.Fields.CONCEPT_CHANGES_COMPONENT_CHANGES}));
		}

		final SearchHits<Activity> search = elasticsearchOperations.search(queryBuilder.withPageable(page).build(), Activity.class);

		Page<Activity> results = new PageImpl<>(search.stream().map(SearchHit::getContent).toList(), page, search.getTotalHits());
		if (!request.isBrief() && !request.isSummaryOnly()) {
			filterResultsBy(request.getConceptId(), request.getComponentId(), results);
		}
		return results;
	}

	private void doBranchFiltering(BoolQuery.Builder query, ActivitySearchRequest request) {
		if (request.getOriginalBranch() != null && !request.getOriginalBranch().isEmpty()) {
			query.must(QueryHelper.termQuery(Activity.Fields.BRANCH, request.getOriginalBranch()));
		}
		if (request.getOnBranch() != null && !request.getOnBranch().isEmpty()) {
			BoolQuery.Builder boolQuery = bool();
			// One of these conditions must be true.  Either
			boolQuery.should(QueryHelper.termQuery(Activity.Fields.BRANCH, request.getOnBranch()))
					// Or
					.should(QueryHelper.termQuery(Activity.Fields.HIGHEST_PROMOTED_BRANCH, request.getOnBranch()));
			//Are we also checking for activity that might have been promoted higher, elsewhere?
			if (request.isIncludeHigherPromotions()) {
				for (String higherBranch : BranchUtils.getAncestorBranches(request.getOnBranch())) {
					boolQuery.should(QueryHelper.termQuery(Activity.Fields.HIGHEST_PROMOTED_BRANCH, higherBranch));
				}
			}
			query.must(List.of(new Query(boolQuery.build())));
		}
		if (request.getSourceBranch() != null && !request.getSourceBranch().isEmpty()) {
			query.must(QueryHelper.termQuery(Activity.Fields.SOURCE_BRANCH, request.getSourceBranch()));
		}
		if (request.getBranchPrefix() != null && !request.getBranchPrefix().isEmpty()) {
			query.must(QueryHelper.prefixQuery(Activity.Fields.BRANCH, request.getBranchPrefix()));
		}
	}

	private void doContentFiltering(BoolQuery.Builder query, ActivitySearchRequest request) {
		if (request.getActivityType() != null) {
			query.must(QueryHelper.termQuery(Activity.Fields.ACTIVITY_TYPE, request.getActivityType().name()));
		}
		if (request.getConceptId() != null) {
			query.must(QueryHelper.termQuery(Activity.Fields.CONCEPT_CHANGES_CONCEPT_ID, request.getConceptId()));
		}
		if (request.getComponentId() != null) {
			query.must(QueryHelper.termQuery(Activity.Fields.COMPONENT_CHANGES_COMPONENT_ID, request.getComponentId()));
		}
		if (request.getCommitDate() != null) {
			query.must(QueryHelper.termQuery(Activity.Fields.COMMIT_DATE, request.getCommitDate().getTime()));
		}

		if (request.getFromDate() != null || request.getToDate() != null) {
			RangeQuery.Builder rangeQuery = QueryHelper.rangeQueryBuilder(Activity.Fields.COMMIT_DATE);
			if (request.getFromDate() != null) {
				QueryHelper.withFrom(rangeQuery, request.getFromDate().getTime());
			}

			if (request.getToDate() != null) {
				QueryHelper.withTo(rangeQuery, request.getToDate().getTime());
			}

			query.must(QueryHelper.toQuery(rangeQuery));
		}

		if (request.isIntOnly()) {
			query.mustNot(QueryHelper.regexQuery(Activity.Fields.BRANCH, ".*SNOMEDCT-.*"));
		}
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
			query.must(QueryHelper.termsQuery(Activity.Fields.CONCEPT_CHANGES_CONCEPT_ID, conceptIds));
		}
		if (activityType != null) {
			query.must(QueryHelper.termQuery(Activity.Fields.ACTIVITY_TYPE, activityType.name()));
		}
		if (user != null && !user.isEmpty()) {
			query.must(QueryHelper.termQuery(Activity.Fields.USERNAME, user));
		}
		NativeQueryBuilder queryBuilder = new NativeQueryBuilder().withQuery(QueryHelper.toQuery(query));
		if (summaryOnly) {
			queryBuilder.withSourceFilter(new FetchSourceFilter(null, new String[]{Activity.Fields.CONCEPT_CHANGES_COMPONENT_CHANGES}));
		}
		final SearchHits<Activity> search = elasticsearchOperations.search(queryBuilder.withPageable(page).build(), Activity.class);

		Page<Activity> results = new PageImpl<>(search.stream().map(SearchHit::getContent).toList(), page, search.getTotalHits());

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

	public Page<Activity> findActivitiesBy(String componentSubType, String usersStr, String branchesStr, Date since, Pageable page) {
		final BoolQuery.Builder query = bool();

		query.must(QueryHelper.termQuery(Activity.Fields.ACTIVITY_TYPE, ActivityType.CONTENT_CHANGE.name()));

		if (componentSubType != null && !componentSubType.isEmpty()) {
			query.must(QueryHelper.termQuery(Activity.Fields.COMPONENT_CHANGES_COMPONENT_SUB_TYPE, componentSubType));
		}

		if (usersStr != null && !usersStr.isEmpty()) {
			List<String> users = Splitter.on(",").trimResults().splitToList(usersStr);
			query.must(QueryHelper.termsQuery(Activity.Fields.USERNAME, users));
		}

		if (branchesStr != null && !branchesStr.isEmpty()) {
			List<String> branches = Splitter.on(",").trimResults().splitToList(branchesStr);
			BoolQuery.Builder branchesQuery = bool();
			for (String branch : branches) {
				branchesQuery.should(QueryHelper.prefixQuery(Activity.Fields.SOURCE_BRANCH, branch));
			}
			query.must(Query.of(q -> q.bool(branchesQuery.build())));
		}

		if (since != null) {
			query.must(Query.of(q -> q.range(QueryHelper.rangeQueryBuilder(Activity.Fields.COMMIT_DATE).gte(JsonData.of(since.getTime())).build())));
		}

		NativeQueryBuilder queryBuilder = new NativeQueryBuilder().withQuery(QueryHelper.toQuery(query));

		queryBuilder.withSourceFilter(new FetchSourceFilter(null, new String[]{Activity.Fields.CONCEPT_CHANGES_COMPONENT_CHANGES}));

		final SearchHits<Activity> search = elasticsearchOperations.search(queryBuilder.withPageable(page).build(), Activity.class);

		return new PageImpl<>(search.stream().map(SearchHit::getContent).toList(), page, search.getTotalHits());
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
