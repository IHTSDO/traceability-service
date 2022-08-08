package org.ihtsdo.otf.traceabilityservice.service;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.ihtsdo.otf.traceabilityservice.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHitsIterator;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
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

	private static final Logger LOGGER = LoggerFactory.getLogger(ReportService.class);

	public ChangeSummaryReport createChangeSummaryReport(String branch) {
		return createChangeSummaryReport(branch, true, true, true);
	}

	public ChangeSummaryReport createChangeSummaryReport(String branch, boolean includeMadeOnThisBranch, boolean includePromotedToThisBranch, boolean includeRebasedToThisBranch) {

		Map<ComponentType, Set<String>> componentChanges = new EnumMap<>(ComponentType.class);
		List<Activity> changesNotAtTaskLevel = new ArrayList<>();
		Map<String, String> componentToConceptIdMap = new HashMap<>();

		if (includeRebasedToThisBranch && !BranchUtil.isCodeSystemBranch(branch)) {
			// Changes made on ancestor branches, starting with the parent branch and working up.
			final Deque<String> ancestors = createAncestorDeque(branch);
			String previousLevel = branch;
			while (!ancestors.isEmpty()) {
				// Select content on this level, promoted before last rebase
				// Only need to set start date if code system branch using last version commit
				final String ancestor = ancestors.pop();
				Date previousLevelBaseDate = getBaseDateUsingBestGuess(previousLevel);
				Date startDate = getStartDate(ancestor, previousLevelBaseDate);
				// Changes made on ancestor branches, rebased to this one
				final BoolQueryBuilder onAncestorBranch =
						boolQuery()
								// Changes made on ancestor
								.should(boolQuery()
										.must(termQuery(Activity.Fields.branch, ancestor))
										.must(rangeQuery(Activity.Fields.commitDate)
												.gt(startDate.getTime())
												.lte(previousLevelBaseDate.getTime())))
								// Changes promoted to ancestor
								.should(boolQuery()
										.must(termQuery(Activity.Fields.highestPromotedBranch, ancestor))
										.must(rangeQuery(Activity.Fields.promotionDate)
												.gt(startDate.getTime())
												.lte(previousLevelBaseDate.getTime())));
				processCommits(onAncestorBranch, componentChanges, changesNotAtTaskLevel, componentToConceptIdMap);

				previousLevel = ancestor;
				if (BranchUtil.isCodeSystemBranch(ancestor)) {
					// Stop at any code system level
					// We don't expect to inherit unversioned content from other code systems.
					break;
				}
			}
		}

		if (includePromotedToThisBranch) {
			// Changes made on child branches, promoted to this one
			// Unlike rebase; We don't need to lookup the last promotion activity here
			// because the service keeps track of what was promoted and when.
			Date startDate = getStartDate(branch, new Date());
			LOGGER.info("select promotion activities after {} ({}) on branch {}", startDate.getTime(), startDate, branch);
			final BoolQueryBuilder onDescendantBranches = boolQuery()
					.mustNot(termQuery(Activity.Fields.branch, branch))
					.must(termQuery(Activity.Fields.highestPromotedBranch, branch))
					.must(rangeQuery(Activity.Fields.promotionDate).gt(startDate.getTime()));
			processCommits(onDescendantBranches, componentChanges, changesNotAtTaskLevel, componentToConceptIdMap);
		}

		if (includeMadeOnThisBranch) {
			Date startDate = getStartDate(branch, new Date());
			LOGGER.debug("startDate {}", startDate);
			// Changes made on this branch
			final BoolQueryBuilder onThisBranchQuery = boolQuery()
					.must(termQuery(Activity.Fields.branch, branch))
					.must(termQuery(Activity.Fields.highestPromotedBranch, branch))// This means not promoted yet
					.must(rangeQuery(Activity.Fields.commitDate).gt(startDate.getTime()));
			processCommits(onThisBranchQuery, componentChanges, changesNotAtTaskLevel, componentToConceptIdMap);
		}

		componentChanges.entrySet().removeIf(entry -> entry.getValue().isEmpty());

		ChangeSummaryReport changeSummaryReport = new ChangeSummaryReport(componentChanges, changesNotAtTaskLevel);
		if (!componentChanges.isEmpty()) {
			changeSummaryReport.setComponentToConceptIdMap(componentToConceptIdMap);
		}
		return changeSummaryReport;
	}

	private Date getStartDate(String branch, Date previousLevelBaseDate) {
		Date startDate;
		if (BranchUtil.isCodeSystemBranch(branch)) {
			startDate = getLastVersionDateOrEpoch(branch, previousLevelBaseDate);
		} else {
			// Changes after last promotion date should be selected on current branch. Changes promoted before will be part of ancestor branch.
			startDate = getLastPromotionDate(branch);
		}
		return startDate;
	}


	private void processCommits(BoolQueryBuilder selection, Map<ComponentType, Set<String>> componentChanges,
							List<Activity> changesNotAtTaskLevel, Map<String, String> componentToConceptMap) {
		NativeSearchQuery query = new NativeSearchQueryBuilder().withQuery(selection)
				// Use 1000 instead of 10_000 because each activity doc containing all changes which can be very large
				.withPageable(PageRequest.of(0, 1_000, Sort.by(Activity.Fields.promotionDate, Activity.Fields.commitDate)))
				.build();
		Map<String, Stack<ComponentChange>> changeStack = new HashMap<>();
		try (SearchHitsIterator<Activity> stream = elasticsearchRestTemplate.searchForStream(query, Activity.class)) {
			stream.forEachRemaining(hit -> {
				final Activity activity = hit.getContent();
				if (activity.getActivityType() == ActivityType.CONTENT_CHANGE && activity.getBranchDepth() != 3 && !PatchService.HISTORY_PATCH_USERNAME.equals(activity.getUsername())) {
					changesNotAtTaskLevel.add(activity);
				}
				// Branch locking during promotion makes sure that activities from different branches can not be promoted at the same time.
				if (!changeStack.isEmpty() && !changeStack.containsKey(activity.getBranch())) {
					processChangeStack(componentChanges, componentToConceptMap, changeStack.values());
					changeStack.clear();
				}
				// Collect changes stack for a given branch to work out superseded changes
				Stack<ComponentChange> changes = changeStack.computeIfAbsent(activity.getBranch(), stack -> new Stack<>());
				activity.getConceptChanges().forEach(conceptChange -> {
					conceptChange.getComponentChanges().forEach(componentChange -> {
						changes.add(componentChange);
						componentToConceptMap.put(componentChange.getComponentId(), conceptChange.getConceptId());
					});
				});
			});
		}
		// Process remaining change stack
		if (!changeStack.isEmpty()) {
			processChangeStack(componentChanges, componentToConceptMap, changeStack.values());
			changeStack.clear();
		}
	}

	private void processChangeStack(Map<ComponentType, Set<String>> componentChanges, Map<String, String> componentToConceptMap, Collection<Stack<ComponentChange>> changeStacks) {
		Map<String, ComponentChange> changeByComponentId = new HashMap<>();
		changeStacks.forEach(stack -> {
			while (!stack.isEmpty()) {
				ComponentChange change = stack.pop();
				// Only keep the most recent change from stack
				if (changeByComponentId.containsKey(change.getComponentId())) {
					continue;
				}
				changeByComponentId.put(change.getComponentId(), change);
			}
			changeByComponentId.values().forEach(componentChange -> {
				if (!componentChange.isSuperseded()) {
					final Set<String> ids = componentChanges.computeIfAbsent(componentChange.getComponentType(), type -> new HashSet<>());
					if (!componentChange.isEffectiveTimeNull() || componentChange.getChangeType() == ChangeType.DELETE) {
						// new commit may have restored effectiveTime or component change is deleted.
						// remove component id from set because we no longer expect a row in the delta
						ids.remove(componentChange.getComponentId());
						componentToConceptMap.remove(componentChange.getComponentId());
					} else {
						ids.add(componentChange.getComponentId());
					}
				}
			});
		});
	}

	private Date getLastVersionDateOrEpoch(String branch, Date before) {
		// If Code System branch; use the last version creation date, because versioning sets all the effectiveTimes so delta would be empty at that point.
		if (BranchUtil.isCodeSystemBranch(branch)) {
			final BoolQueryBuilder query = boolQuery()
					.must(termQuery(Activity.Fields.activityType, ActivityType.CREATE_CODE_SYSTEM_VERSION))
					.must(termQuery(Activity.Fields.branch, branch))
					.must(rangeQuery(Activity.Fields.commitDate).lt(before.getTime()));
			final SearchHit<Activity> activityHit = elasticsearchRestTemplate.searchOne(new NativeSearchQueryBuilder().withQuery(query).build()
					.addSort(Sort.by(Activity.Fields.commitDate).descending()), Activity.class);
			if (activityHit != null) {
				return activityHit.getContent().getCommitDate();
			}
		}
		return EPOCH_DATE;
	}

	private Date getBaseDateUsingBestGuess(String branch) {
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

		if (activitySearchHit != null) {
			return activitySearchHit.getContent().getCommitDate();
		} else {
			// Select first commit on the branch
			final SearchHit<Activity> firstCommitSearchHit = elasticsearchRestTemplate.searchOne(new NativeSearchQueryBuilder()
					.withQuery(termQuery(Activity.Fields.branch, branch))
					.withPageable(MOST_RECENT_COMMIT)
					.build(), Activity.class);
			if (firstCommitSearchHit != null) {
				return firstCommitSearchHit.getContent().getCommitDate();
			} else {
				return new Date();
			}
		}
	}

	private Date getLastPromotionDate(String branch) {
		final SearchHit<Activity> activitySearchHit = elasticsearchRestTemplate.searchOne(new NativeSearchQueryBuilder()
				.withQuery(boolQuery()
						.must(termQuery(Activity.Fields.sourceBranch, branch))
						.must(termQuery(Activity.Fields.activityType, ActivityType.PROMOTION))
				)
				.withPageable(MOST_RECENT_COMMIT)
				.build(), Activity.class);

		// if never promoted use epoch
		return activitySearchHit == null ? EPOCH_DATE : activitySearchHit.getContent().getCommitDate();
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
