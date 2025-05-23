package org.ihtsdo.otf.traceabilityservice.service;

import co.elastic.clients.elasticsearch._types.query_dsl.*;
import org.ihtsdo.otf.traceabilityservice.util.QueryHelper;
import org.ihtsdo.otf.traceabilityservice.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.client.elc.NativeQueryBuilder;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHitsIterator;
import org.springframework.stereotype.Service;

import java.util.*;

import static co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders.bool;

@Service
public class ReportService {

	public static final PageRequest MOST_RECENT_COMMIT = PageRequest.of(0, 1, Sort.by(Activity.Fields.COMMIT_DATE).descending());

	private static final Date EPOCH_DATE = new Date(0);

	@Autowired
	private ElasticsearchOperations elasticsearchOperations;

	private static final Logger LOGGER = LoggerFactory.getLogger(ReportService.class);

	public ChangeSummaryReport createChangeSummaryReport(String branch) {
		return createChangeSummaryReport(branch, null, null, true, true, true);
	}

	public ChangeSummaryReport createChangeSummaryReport(String branch, Long contentBaseTimeStamp, Long contentHeadTimestamp) {
		return createChangeSummaryReport(branch, contentBaseTimeStamp, contentHeadTimestamp, true, true, true);
	}

	public ChangeSummaryReport createChangeSummaryReport(String branch, Long contentBaseTimeStamp, Long contentHeadTimestamp, boolean includeMadeOnThisBranch, boolean includePromotedToThisBranch, boolean includeRebasedToThisBranch) {
		Map<String, ComponentChange> componentChangeMap = new HashMap<>();
		List<Activity> changesNotAtTaskLevel = new ArrayList<>();
		Map<String, String> componentToConceptIdMap = new HashMap<>();
		Date startDate = getStartDate(branch, contentHeadTimestamp != null ? new Date(contentHeadTimestamp) : new Date());
		if (contentHeadTimestamp != null) {
			LOGGER.info("selecting changes with cut off time {} ({}) on branch {}", contentHeadTimestamp, new Date(contentHeadTimestamp), branch);
		}
		if (includePromotedToThisBranch && includeMadeOnThisBranch) {
			// Changes made on a branch will have the highestPromotedBranch set to itself initially
			// Process changes promoted and made on project together to avoid false positive due to conflict changes
			// e.g relationship created on project during classification save but deleted by a task promoted to project
			LOGGER.info("selecting changes promoted/committed after {} ({}) on branch {}", startDate.getTime(), startDate, branch);

			RangeQuery.Builder rangeQueryBuilder = QueryHelper.rangeQueryBuilder(Activity.Fields.PROMOTION_DATE);
			QueryHelper.withFrom(rangeQueryBuilder, startDate.getTime());
			if (contentHeadTimestamp != null) {
				QueryHelper.withTo(rangeQueryBuilder, contentHeadTimestamp);
			}

			final BoolQuery.Builder query = bool()
					.must(QueryHelper.termQuery(Activity.Fields.HIGHEST_PROMOTED_BRANCH, branch))
					.must(QueryHelper.toQuery(rangeQueryBuilder));
			processCommits(query, componentChangeMap, changesNotAtTaskLevel, componentToConceptIdMap);
		} else if (includePromotedToThisBranch) {
			// Changes made on child branches, promoted to this one only
			if (contentBaseTimeStamp != null && contentBaseTimeStamp > startDate.getTime()) {
				startDate = new Date(contentBaseTimeStamp);
			}

			RangeQuery.Builder rangeQueryBuilder = QueryHelper.rangeQueryBuilder(Activity.Fields.PROMOTION_DATE);
			QueryHelper.withFrom(rangeQueryBuilder, startDate.getTime());
			if (contentHeadTimestamp != null) {
				QueryHelper.withTo(rangeQueryBuilder, contentHeadTimestamp);
			}
			LOGGER.info("selecting changes promoted after {} ({}) on branch {}", startDate.getTime(), startDate, branch);
			final BoolQuery.Builder onDescendantBranches = bool()
					.mustNot(QueryHelper.termQuery(Activity.Fields.BRANCH, branch))
					.must(QueryHelper.termQuery(Activity.Fields.HIGHEST_PROMOTED_BRANCH, branch))
					.must(QueryHelper.toQuery(rangeQueryBuilder));
			processCommits(onDescendantBranches, componentChangeMap, changesNotAtTaskLevel, componentToConceptIdMap);
		} else if (includeMadeOnThisBranch) {
			// Changes made on this branch only
			if (contentBaseTimeStamp != null && contentBaseTimeStamp > startDate.getTime()) {
				startDate = new Date(contentBaseTimeStamp);
			}
			LOGGER.info("selecting changes committed after {} ({}) on branch {}", startDate.getTime(), startDate, branch);
			RangeQuery.Builder rangeQueryBuilder = QueryHelper.rangeQueryBuilder(Activity.Fields.COMMIT_DATE);
			QueryHelper.withFrom(rangeQueryBuilder, startDate.getTime());
			if (contentHeadTimestamp != null) {
				QueryHelper.withTo(rangeQueryBuilder, contentHeadTimestamp);
			}
			final BoolQuery.Builder onThisBranchQuery = bool()
					.must(QueryHelper.termQuery(Activity.Fields.BRANCH, branch))
					.must(QueryHelper.toQuery(rangeQueryBuilder));
			processCommits(onThisBranchQuery, componentChangeMap, changesNotAtTaskLevel, componentToConceptIdMap);
		}

		if (includeRebasedToThisBranch) {
			processChangesRebasedToBranch(branch, contentBaseTimeStamp, componentChangeMap, changesNotAtTaskLevel, componentToConceptIdMap);
		}

		Map<ComponentType, Set<String>> componentChanges = processComponentChanges(componentChangeMap.values(), componentToConceptIdMap);

		componentChanges.entrySet().removeIf(entry -> entry.getValue().isEmpty());

		ChangeSummaryReport changeSummaryReport = new ChangeSummaryReport(componentChanges, changesNotAtTaskLevel);
		if (!componentChanges.isEmpty()) {
			changeSummaryReport.setComponentToConceptIdMap(componentToConceptIdMap);
		}
		return changeSummaryReport;
	}

	private Map<ComponentType, Set<String>> processComponentChanges(Collection<ComponentChange> changeSet, Map<String, String> componentToConceptMap) {
		Map<ComponentType, Set<String>> componentChanges = new EnumMap<>(ComponentType.class);
		changeSet.forEach(componentChange -> {
			final Set<String> ids = componentChanges.computeIfAbsent(componentChange.getComponentType(), type -> new HashSet<>());
			if (componentChange.getChangeType() == ChangeType.DELETE) {
				componentToConceptMap.remove(componentChange.getComponentId());
			} else {
				if (componentChange.isEffectiveTimeNull()) {
					ids.add(componentChange.getComponentId());
				} else {
					// new commit may have restored effectiveTime,
					// remove component id from set because we no longer expect a row in the delta
					componentToConceptMap.remove(componentChange.getComponentId());
				}
			}
		});
		return componentChanges;
	}

	private void processChangesRebasedToBranch(String branch, Long contentBaseTimeStamp, Map<String, ComponentChange> componentChangeMap,
											   List<Activity> changesNotAtTaskLevel, Map<String, String> componentToConceptIdMap) {
		if (!BranchUtils.isCodeSystemBranch(branch)) {
			if (contentBaseTimeStamp != null) {
				LOGGER.info("Processing rebased changes with base time {} ({}) on branch {}", contentBaseTimeStamp, new Date(contentBaseTimeStamp), branch);
			}
			// Changes made on ancestor branches, starting with the parent branch and working up.
			final Deque<String> ancestors = createAncestorDeque(branch);
			String previousLevel = branch;
			Date previousLevelBaseDate = null;
			while (!ancestors.isEmpty()) {
				// Select content on this level, promoted before last rebase
				// Only need to set start date if code system branch using last version commit
				final String ancestor = ancestors.pop();
				// previousLevelBaseDate is the head time for current ancestor
				if (previousLevelBaseDate == null) {
					previousLevelBaseDate = contentBaseTimeStamp != null ? new Date(contentBaseTimeStamp) : getBaseDateUsingBestGuess(branch);
				} else {
					previousLevelBaseDate = getBaseDate(previousLevel, previousLevelBaseDate);
				}
				Date startDate = getStartDate(ancestor, previousLevelBaseDate);
				LOGGER.info("Selecting changes from {}({}) to {}({}) on branch {}", startDate, startDate.getTime(), previousLevelBaseDate, previousLevelBaseDate.getTime(), ancestor);
				// Changes made on ancestor branches, rebased to this one
				final BoolQuery.Builder onAncestorBranch =
						bool()
								// Changes made on ancestor
								.should(QueryHelper.toQuery(bool()
										.must(QueryHelper.termQuery(Activity.Fields.BRANCH, ancestor))
										.must(QueryHelper.rangeQuery(Activity.Fields.COMMIT_DATE, startDate.getTime(), previousLevelBaseDate.getTime()))))
								// Changes promoted to ancestor
								.should(QueryHelper.toQuery(bool()
										.must(QueryHelper.termQuery(Activity.Fields.HIGHEST_PROMOTED_BRANCH, ancestor))
										.must(QueryHelper.rangeQuery(Activity.Fields.PROMOTION_DATE, startDate.getTime(), previousLevelBaseDate.getTime()))));
				processCommits(onAncestorBranch, componentChangeMap, changesNotAtTaskLevel, componentToConceptIdMap);

				previousLevel = ancestor;
				if (BranchUtils.isCodeSystemBranch(ancestor)) {
					// Stop at any code system level
					// We don't expect to inherit unversioned content from other code systems.
					break;
				}
			}
		}
	}

	private Date getStartDate(String branch, Date previousLevelBaseDate) {
		Date startDate;
		if (BranchUtils.isCodeSystemBranch(branch)) {
			startDate = getLastVersionDateOrEpoch(branch, previousLevelBaseDate);
		} else {
			// Changes after last promotion date should be selected on current branch. Changes promoted before will be part of ancestor branch.
			startDate = getLastPromotionDate(branch, previousLevelBaseDate);
		}
		return startDate;
	}

	private void processCommits(BoolQuery.Builder selection, Map<String, ComponentChange> componentChangeMap,
	                            List<Activity> changesNotAtTaskLevel, Map<String, String> componentToConceptMap) {
		NativeQuery query = new NativeQueryBuilder().withQuery(QueryHelper.toQuery(selection))
				// Use 1000 instead of 10_000 because each activity doc containing all changes which can be very large
				// Sort by descending order to discard superseded changes
				.withPageable(PageRequest.of(0, 1_000, Sort.by(Sort.Direction.DESC, Activity.Fields.COMMIT_DATE)))
				.build();
		final Map<String, Set<String>> supersededChangeComponentToPaths = new HashMap<>();
		try (SearchHitsIterator<Activity> stream = elasticsearchOperations.searchForStream(query, Activity.class)) {
			stream.forEachRemaining(hit -> {
				final Activity activity = hit.getContent();
				if (activity.getActivityType() == ActivityType.CONTENT_CHANGE && activity.getBranchDepth() != 3 && !PatchService.HISTORY_PATCH_USERNAME.equals(activity.getUsername())) {
					changesNotAtTaskLevel.add(activity);
				}
				activity.getConceptChanges().forEach(conceptChange -> {
					final String conceptId = conceptChange.getConceptId();
					conceptChange.getComponentChanges().forEach(componentChange -> {
						if (componentChange.isSuperseded()) {
							supersededChangeComponentToPaths.computeIfAbsent(componentChange.getComponentId(), values -> new HashSet<>()).add(activity.getBranch());
						}
						if (!superseded(supersededChangeComponentToPaths, componentChange.getComponentId(), activity.getBranch())) {
							componentChangeMap.putIfAbsent(componentChange.getComponentId(), componentChange);
							componentToConceptMap.putIfAbsent(componentChange.getComponentId(), conceptId);
						}
					});
				});
			});
		}
	}


	/**
	 * Superseded changes apply only for a given branch. Any changes before this commit on this branch should be ignored
	 * Changes from other branch should apply however if the superseded branch is at project level
	 * and any changes on task before this commit should be ignored too.
	 * @param supersededChangesByComponent superseded component id to paths map
	 * @param componentId componentId
	 * @param branch branch for component change
	 * @return true if superseded
	 */
	private boolean superseded(Map<String, Set<String>> supersededChangesByComponent, String componentId, String branch) {
		if (!supersededChangesByComponent.containsKey(componentId)) {
			return false;
		}
		Set<String> branches = supersededChangesByComponent.getOrDefault(componentId, Collections.emptySet());
		for (String branchPath : branches) {
			// A change is superseded when the branch is the same or child branch of superseded branch path
			if (branchPath.equals(branch) || BranchUtils.getAncestorBranches(branch).contains(branchPath)) {
				return true;
			}
		}
		return false;
	}

	private Date getLastVersionDateOrEpoch(String branch, Date before) {
		// If Code System branch; use the last version creation date, because versioning sets all the effectiveTimes so delta would be empty at that point.
		if (BranchUtils.isCodeSystemBranch(branch)) {
			RangeQuery.Builder rangeQueryBuilder = QueryHelper.rangeQueryBuilder(Activity.Fields.COMMIT_DATE);
			QueryHelper.withTo(rangeQueryBuilder, before.getTime());
			BoolQuery.Builder query = bool()
					.must(QueryHelper.termQuery(Activity.Fields.ACTIVITY_TYPE, ActivityType.CREATE_CODE_SYSTEM_VERSION.name()))
					.must(QueryHelper.termQuery(Activity.Fields.BRANCH, branch))
					.must(QueryHelper.toQuery(rangeQueryBuilder));
			final SearchHit<Activity> activityHit = elasticsearchOperations.searchOne(new NativeQueryBuilder().withQuery(QueryHelper.toQuery(query))
					.withSort(Sort.by(Activity.Fields.COMMIT_DATE).descending())
					.build(), Activity.class);
			if (activityHit != null) {
				return activityHit.getContent().getCommitDate();
			}
		}
		return EPOCH_DATE;
	}

	private Date getBaseDateUsingBestGuess(String branch) {
		final SearchHit<Activity> activitySearchHit = elasticsearchOperations.searchOne(new NativeQueryBuilder()
				.withQuery(QueryHelper.toQuery(bool()
						.should(QueryHelper.toQuery(bool()
								.must(QueryHelper.termQuery(Activity.Fields.BRANCH, branch))
								.must(QueryHelper.termQuery(Activity.Fields.ACTIVITY_TYPE, ActivityType.REBASE.name()))))
						.should(QueryHelper.toQuery(bool()
								.must(QueryHelper.termQuery(Activity.Fields.SOURCE_BRANCH, branch))
								.must(QueryHelper.termQuery(Activity.Fields.ACTIVITY_TYPE, ActivityType.PROMOTION.name()))))))
				.withPageable(MOST_RECENT_COMMIT)
				.build(), Activity.class);

		if (activitySearchHit != null) {
			return activitySearchHit.getContent().getCommitDate();
		} else {
			// Select first commit on the branch
			final SearchHit<Activity> firstCommitSearchHit = elasticsearchOperations.searchOne(new NativeQueryBuilder()
					.withQuery(QueryHelper.termQuery(Activity.Fields.BRANCH, branch))
					.withPageable(MOST_RECENT_COMMIT)
					.build(), Activity.class);
			if (firstCommitSearchHit != null) {
				return firstCommitSearchHit.getContent().getCommitDate();
			} else {
				return new Date();
			}
		}
	}

	private Date getBaseDate(String branch, Date previousLevelBaseDate) {
		if (previousLevelBaseDate == null) {
			throw new IllegalArgumentException("previousLevelBaseDate can't be null");
		}

		RangeQuery.Builder rangeQueryBuilder = QueryHelper.rangeQueryBuilder(Activity.Fields.COMMIT_DATE);
		QueryHelper.withTo(rangeQueryBuilder, previousLevelBaseDate.getTime());
		final SearchHit<Activity> activitySearchHit = elasticsearchOperations.searchOne(new NativeQueryBuilder()
				.withQuery(QueryHelper.toQuery(bool()
						.should(QueryHelper.toQuery(bool()
								.must(QueryHelper.termQuery(Activity.Fields.BRANCH, branch))
								.must(QueryHelper.termQuery(Activity.Fields.ACTIVITY_TYPE, ActivityType.REBASE.name()))))
						.should(QueryHelper.toQuery(bool()
								.must(QueryHelper.termQuery(Activity.Fields.SOURCE_BRANCH, branch))
								.must(QueryHelper.termQuery(Activity.Fields.ACTIVITY_TYPE, ActivityType.PROMOTION.name()))))
						.must(QueryHelper.toQuery(rangeQueryBuilder)))
				)
				.withPageable(MOST_RECENT_COMMIT)
				.build(), Activity.class);

		if (activitySearchHit != null) {
			return activitySearchHit.getContent().getCommitDate();
		} else {
			return previousLevelBaseDate;
		}
	}

	private Date getLastPromotionDate(String branch, Date baseDateTime) {
		RangeQuery.Builder rangeQueryBuilder = QueryHelper.rangeQueryBuilder(Activity.Fields.COMMIT_DATE);
		QueryHelper.withTo(rangeQueryBuilder, baseDateTime.getTime());
		final SearchHit<Activity> activitySearchHit = elasticsearchOperations.searchOne(new NativeQueryBuilder()
				.withQuery(QueryHelper.toQuery(bool()
						.must(QueryHelper.termQuery(Activity.Fields.SOURCE_BRANCH, branch))
						.must(QueryHelper.termQuery(Activity.Fields.ACTIVITY_TYPE, ActivityType.PROMOTION.name()))
						.must(QueryHelper.toQuery(rangeQueryBuilder))))
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
			if (!ancestor.isEmpty()) {
				ancestor.append("/");
			}
			ancestor.append(part);
			ancestors.push(ancestor.toString());
		}
		ancestors.pop();

		return ancestors;
	}
}
