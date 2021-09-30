package org.ihtsdo.otf.traceabilityservice.service;

import com.google.common.collect.Sets;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.ihtsdo.otf.traceabilityservice.domain.*;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHitsIterator;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.stereotype.Service;

import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.*;

@Service
public class ReportService {

	public static final PageRequest MOST_RECENT_COMMIT = PageRequest.of(0, 1, Sort.by(Activity.Fields.commitDate).descending());

	private static final Date EPOCH_DATE = new Date(0);

	@Autowired
	private ElasticsearchRestTemplate elasticsearchRestTemplate;

	public ChangeSummaryReport createChangeSummaryReport(String branch) {
		return createChangeSummaryReport(branch, true, true, true);
	}

	public ChangeSummaryReport createChangeSummaryReport(String branch, boolean includeMadeOnThisBranch, boolean includePromotedToThisBranch, boolean includeRebasedToThisBranch) {

		Map<ComponentType, Set<String>> componentChanges = new EnumMap<>(ComponentType.class);
		List<Activity> changesNotAtTaskLevel = new ArrayList<>();

		if (includeRebasedToThisBranch && !BranchUtil.isCodeSystemBranch(branch)) {
			// Changes made on ancestor branches
			final Deque<String> ancestors = createAncestorDeque(branch);
			String previousLevel = branch;
			while (!ancestors.isEmpty()) {
				// Select content on this level, promoted before last rebase
				// Only need to set start date if code system branch = last versioning
				final String ancestor = ancestors.pop();
				Date lastRebaseDate = getLastRebaseOrPromotionDate(previousLevel);
				Date changeStartDate = getChangeStartDate(ancestor, lastRebaseDate);

				// Changes made on ancestor branches, rebased to this one
				final BoolQueryBuilder onAncestorBranch =
						boolQuery()
								// Changes made on ancestor
								.should(boolQuery()
										.must(termQuery(Activity.Fields.branch, ancestor))
										.must(rangeQuery(Activity.Fields.commitDate)
												.gt(changeStartDate.getTime())
												.lte(lastRebaseDate.getTime())))
								// Changes promoted to ancestor
								.should(boolQuery()
										.must(termQuery(Activity.Fields.highestPromotedBranch, ancestor))
										.must(rangeQuery(Activity.Fields.promotionDate)
												.gt(changeStartDate.getTime())
												.lte(lastRebaseDate.getTime())));
				processCommits(onAncestorBranch, componentChanges, changesNotAtTaskLevel);

				previousLevel = ancestor;
				if (BranchUtil.isCodeSystemBranch(ancestor)) {
					// Stop at any code system level
					// We don't expect to inherit unversioned content from other code systems.
					break;
				}
			}
		}

		Date changeStartDate = getChangeStartDate(branch);

		if (includePromotedToThisBranch) {
			// Changes made on child branches, promoted to this one
			// Unlike rebase; We don't need to lookup the last promotion activity here
			// because the service keeps track of what has promoted and when
			final BoolQueryBuilder onDescendantBranches = boolQuery()
					.mustNot(termQuery(Activity.Fields.branch, branch))
					.must(termQuery(Activity.Fields.highestPromotedBranch, branch))
					.must(rangeQuery(Activity.Fields.promotionDate).gte(changeStartDate.getTime()));
			processCommits(onDescendantBranches, componentChanges, changesNotAtTaskLevel);
		}

		if (includeMadeOnThisBranch) {
			// Changes made on this branch
			final BoolQueryBuilder onThisBranchQuery = boolQuery()
					.must(termQuery(Activity.Fields.branch, branch))
					.must(termQuery(Activity.Fields.highestPromotedBranch, branch))
					.must(rangeQuery(Activity.Fields.commitDate).gte(changeStartDate.getTime()));
			processCommits(onThisBranchQuery, componentChanges, changesNotAtTaskLevel);
		}

		componentChanges.entrySet().removeIf(entry -> entry.getValue().isEmpty());

		return new ChangeSummaryReport(componentChanges, changesNotAtTaskLevel);
	}

	private void processCommits(BoolQueryBuilder selection, Map<ComponentType, Set<String>> componentChanges, List<Activity> changesNotAtTaskLevel) {
		try (SearchHitsIterator<Activity> stream = elasticsearchRestTemplate.searchForStream(new NativeSearchQueryBuilder()
				.withQuery(selection)
				.withPageable(PageRequest.of(0, 10_000, Sort.by(Activity.Fields.promotionDate, Activity.Fields.commitDate)))
				.build(), Activity.class)) {
			stream.forEachRemaining(hit -> {
				final Activity activity = hit.getContent();
				if (activity.getActivityType() == ActivityType.CONTENT_CHANGE && activity.getBranchDepth() != 3) {
					changesNotAtTaskLevel.add(activity);
				}
				activity.getConceptChanges().stream()
						.flatMap(conceptChange -> conceptChange.getComponentChanges().stream())
						.forEach(componentChange -> {
							final Set<String> ids = componentChanges.computeIfAbsent(componentChange.getComponentType(), type -> new HashSet<>());

							if (componentChange.getChangeType() == ChangeType.DELETE) {
								ids.remove(componentChange.getComponentId());
							} else {
								if (componentChange.isEffectiveTimeNull()) {
									ids.add(componentChange.getComponentId());
								} else {
									// new commit may have restored effectiveTime,
									// remove component id from set because we no longer expect a row in the delta
									ids.remove(componentChange.getComponentId());
								}
							}
						});
			});
		}
	}

	private Date getChangeStartDate(String branch) {
		return getChangeStartDate(branch, null);
	}

	private Date getChangeStartDate(String branch, Date beforeRebaseDate) {
		// If Code System branch; use the last version creation date, because versioning sets all the effectiveTimes so delta would be empty at that point.
		if (BranchUtil.isCodeSystemBranch(branch)) {
			if (beforeRebaseDate == null) {
				beforeRebaseDate = new Date();
			}

			final BoolQueryBuilder query = boolQuery()
					.must(termQuery(Activity.Fields.activityType, ActivityType.CREATE_CODE_SYSTEM_VERSION))
					.must(termQuery(Activity.Fields.branch, branch))
					.must(rangeQuery(Activity.Fields.commitDate).lt(beforeRebaseDate.getTime()));
			final SearchHit<Activity> activityHit = elasticsearchRestTemplate.searchOne(new NativeSearchQueryBuilder().withQuery(query).build()
					.addSort(Sort.by(Activity.Fields.commitDate).descending()), Activity.class);
			if (activityHit != null) {
				return activityHit.getContent().getCommitDate();
			} else {
				return EPOCH_DATE;
			}
		} else {
			return EPOCH_DATE;
		}
	}

	private Date getLastRebaseOrPromotionDate(String branch) {
		final SearchHit<Activity> activitySearchHit = elasticsearchRestTemplate.searchOne(new NativeSearchQueryBuilder()
				.withQuery(boolQuery()
						.should(
								boolQuery()
										.must(termQuery(Activity.Fields.branch, branch))
										.must(termQuery(Activity.Fields.activityType, ActivityType.REBASE)))
						.should(
								boolQuery()
										.must(termQuery(Activity.Fields.sourceBranch, branch))
										.must(termQuery(Activity.Fields.activityType, ActivityType.PROMOTION)))
				)
				.withPageable(MOST_RECENT_COMMIT)
				.build(), Activity.class);

		// if empty, assume a new branch that is up to date
		return activitySearchHit == null ? new Date() : activitySearchHit.getContent().getCommitDate();
	}

	Deque<String> createAncestorDeque(String branch) {
		final Deque<String> ancestors = new ArrayDeque<>();

		final List<String> split = List.of(branch.split("/"));
		StringBuilder ancestor = new StringBuilder();
		for (String part : split) {
			if (ancestor.length() > 0) {
				ancestor.append("/");
			}
			ancestor.append(part);
			ancestors.push(ancestor.toString());
		}
		ancestors.pop();

		return ancestors;
	}
}
