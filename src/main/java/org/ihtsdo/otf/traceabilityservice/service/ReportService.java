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
		return createChangeSummaryReport(branch, null, null, true, true, true);
	}

	public ChangeSummaryReport createChangeSummaryReport(String branch, Long contentBaseTimeStamp, Long contentHeadTimestamp) {
		return createChangeSummaryReport(branch, contentBaseTimeStamp, contentHeadTimestamp, true, true, true);
	}

	public ChangeSummaryReport createChangeSummaryReport(String branch, Long contentBaseTimeStamp, Long contentHeadTimestamp, boolean includeMadeOnThisBranch, boolean includePromotedToThisBranch, boolean includeRebasedToThisBranch) {
		Map<String, ComponentChange> componentChangeMap = new HashMap<>();
		List<Activity> changesNotAtTaskLevel = new ArrayList<>();
		Map<String, String> componentToConceptIdMap = new HashMap<>();

		Date startDate = getStartDate(branch, new Date());
		LOGGER.info("select activities promoted/committed after {} ({}) on branch {}", startDate.getTime(), startDate, branch);
		if (contentHeadTimestamp != null) {
			LOGGER.info("selecting activities with cut off time {} ({}) on branch {}", contentHeadTimestamp, new Date(contentHeadTimestamp), branch);
		}
		if (includePromotedToThisBranch && includeMadeOnThisBranch) {
			// Changes made on a branch will have the highestPromotedBranch set to itself initially
			// Process changes promoted and made on project together to avoid false positive due to conflict changes
			// e.g relationship created on project during classification save but deleted by a task promoted to project
			final BoolQueryBuilder query = boolQuery()
					.must(termQuery(Activity.Fields.highestPromotedBranch, branch))
					.must(contentHeadTimestamp != null ? rangeQuery(Activity.Fields.promotionDate).gt(startDate.getTime()).lte(contentHeadTimestamp)
							: rangeQuery(Activity.Fields.promotionDate).gt(startDate.getTime()));
			processCommits(query, componentChangeMap, changesNotAtTaskLevel, componentToConceptIdMap);
		} else if (includePromotedToThisBranch) {
				// Changes made on child branches, promoted to this one only
				final BoolQueryBuilder onDescendantBranches = boolQuery()
						.mustNot(termQuery(Activity.Fields.branch, branch))
						.must(termQuery(Activity.Fields.highestPromotedBranch, branch))
						.must(contentHeadTimestamp != null ? rangeQuery(Activity.Fields.promotionDate).gt(startDate.getTime()).lte(contentHeadTimestamp)
								: rangeQuery(Activity.Fields.promotionDate).gt(startDate.getTime()));
				processCommits(onDescendantBranches, componentChangeMap, changesNotAtTaskLevel, componentToConceptIdMap);
		} else if (includeMadeOnThisBranch) {
				// Changes made on this branch only
				final BoolQueryBuilder onThisBranchQuery = boolQuery()
						.must(termQuery(Activity.Fields.branch, branch))
						.must(termQuery(Activity.Fields.highestPromotedBranch, branch))// This means not promoted yet
						.must(contentHeadTimestamp != null ? rangeQuery(Activity.Fields.commitDate).gt(startDate.getTime()).lte(contentHeadTimestamp)
								: rangeQuery(Activity.Fields.commitDate).gt(startDate.getTime()));
				processCommits(onThisBranchQuery, componentChangeMap, changesNotAtTaskLevel, componentToConceptIdMap);
		}

		if (includeRebasedToThisBranch) {
			if (contentBaseTimeStamp != null) {
				LOGGER.info("Processing rebased changes with base time {} ({}) on branch {}", contentBaseTimeStamp, new Date(contentBaseTimeStamp), branch);
			}
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
		if (!BranchUtil.isCodeSystemBranch(branch)) {
			// Changes made on ancestor branches, starting with the parent branch and working up.
			final Deque<String> ancestors = createAncestorDeque(branch);
			String previousLevel = branch;
			Date previousLevelBaseDate = null;
			boolean firstLevel = true;
			while (!ancestors.isEmpty()) {
				// Select content on this level, promoted before last rebase
				// Only need to set start date if code system branch using last version commit
				final String ancestor = ancestors.pop();
				if (firstLevel) {
					previousLevelBaseDate = contentBaseTimeStamp != null ? new Date(contentBaseTimeStamp) : getBaseDateUsingBestGuess(branch);
					firstLevel = false;
				} else {
					previousLevelBaseDate = getBaseDate(previousLevel, previousLevelBaseDate);
				}
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
				processCommits(onAncestorBranch, componentChangeMap, changesNotAtTaskLevel, componentToConceptIdMap);

				previousLevel = ancestor;
				if (BranchUtil.isCodeSystemBranch(ancestor)) {
					// Stop at any code system level
					// We don't expect to inherit unversioned content from other code systems.
					break;
				}
			}
		}
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

	private void processCommits(BoolQueryBuilder selection, Map<String, ComponentChange> componentChangeMap,
	                            List<Activity> changesNotAtTaskLevel, Map<String, String> componentToConceptMap) {
		NativeSearchQuery query = new NativeSearchQueryBuilder().withQuery(selection)
				// Use 1000 instead of 10_000 because each activity doc containing all changes which can be very large
				// Sort by descending order to discard superseded changes
				.withPageable(PageRequest.of(0, 1_000, Sort.by(Sort.Direction.DESC, Activity.Fields.promotionDate, Activity.Fields.commitDate)))
				.build();
		final Map<String, Set<String>> supersededChangeComponentToPaths = new HashMap<>();
		try (SearchHitsIterator<Activity> stream = elasticsearchRestTemplate.searchForStream(query, Activity.class)) {
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

	private boolean superseded(Map<String, Set<String>> supersededChangesByComponent, String componentId, String branch) {
		if (!supersededChangesByComponent.containsKey(componentId)) {
			return false;
		}
		Set<String> branches = supersededChangesByComponent.getOrDefault(componentId, Collections.emptySet());
		for (String branchPath : branches) {
			if (branchPath.equals(branch) || branch.startsWith(branchPath)) {
				return true;
			}
		}
		return false;
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

	private Date getBaseDate(String branch, Date previousLevelBaseDate) {
		if (previousLevelBaseDate == null) {
			throw new IllegalArgumentException("previousLevelBaseDate can't be null");
		}
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
						.must(rangeQuery(Activity.Fields.commitDate).lte(previousLevelBaseDate.getTime()))
				)
				.withPageable(MOST_RECENT_COMMIT)
				.build(), Activity.class);

		if (activitySearchHit != null) {
			return activitySearchHit.getContent().getCommitDate();
		} else {
			return previousLevelBaseDate;
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
