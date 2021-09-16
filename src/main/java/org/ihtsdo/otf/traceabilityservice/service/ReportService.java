package org.ihtsdo.otf.traceabilityservice.service;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.ihtsdo.otf.traceabilityservice.domain.*;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.SearchHitsIterator;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.stereotype.Service;

import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.*;

@Service
public class ReportService {

	public static final PageRequest MOST_RECENT_COMMIT = PageRequest.of(0, 1, Sort.by(Activity.Fields.commitDate).descending());
	@Autowired
	private ElasticsearchRestTemplate elasticsearchRestTemplate;

//	@Autowired
//	private SnowstormRestClientFactory snowstormRestClientFactory;

	/*

	Test what happens if:
		- change made in task A to clear effectiveTime
		- change made in task B to clear and restore time, promoted
		- task A rebased - right hand side picked - left hand side picked
		... no merge screen for Syn descriptions - rebase component change is deletion! Bug... fixed
		.. if other changes then merge is logged as an update to all components - the correct isEffectiveTimeNull flag is applied


	THIRD PASS of thinking
		Components that must be in the archive
			- Changes on this branch (if code system branch then it's since last versioning)
		Components that may be in the archive
			- Changes on ancestor branches

		for each branch we know
			- when last rebased and promoted but not when branch created..

		Timespans

		Content on project:
			identify promoted projects S:latest version E:last project rebase
				identify promoted tasks S:previous project promotion (or null) E:project promotion
					identify changes on task S:previous task promotion (or null) E:task promotion

		Content on any branch:
			recursively:
				Select content changes on this branch
					Start date: previous promotion, previous version or null
					End date: none
				Select content changes from child branches
					if not code system just use the onBranch shortcut
					if code system use onBranch with promoted date filter - start date is versioning
				Select content from ancestor branches
					for each, use onBranch shortcut with promoted date filter
					S: code system version or none
					E: last rebase

	 */

	// Q - If effectiveTime made null on this branch then made not null on child branch and promoted.. how do we know which?
	// A - if all activities have a promotion date, sort by that and then commit date.
	//     We want oldest commits first, then promoted content with oldest commits first,
	//     this way the last change or last promoted change will be found last and kept

		/*
		Hard case .
		concept versioned on MAIN
		concept changed on MAIN/A
		concept changed and changed back on MAIN/B
		A promoted
		B rebased .. will record correct date flag
		B promoted
		.. what's in the log
		last commit will have correct flag because it's the rebase commit that happened afterwards
		 */

	public ChangeSummaryReport createChangeSummaryReport(String branch) {
		return createChangeSummaryReport(branch, true, true, true);
	}

	public ChangeSummaryReport createChangeSummaryReport(String branch, boolean includeMadeOnThisBranch, boolean includePromotedToThisBranch, boolean includeRebasedToThisBranch) {

		Map<ComponentType, Set<String>> componentChanges = new EnumMap<>(ComponentType.class);
		List<Activity> changesNotAtTaskLevel = new ArrayList<>();

		Date activityStartDate = getChangeStartDate(branch);

		if (includeRebasedToThisBranch) {
			// Changes made on ancestor branches
			final Deque<String> ancestors = createAncestorDeque(branch);
			String previousLevel = branch;
			while (!ancestors.isEmpty()) {
				// Select content on this level, promoted before last rebase
				// Only need to set start date if code system branch = last versioning
				final String ancestor = ancestors.pop();
				Date lastRebaseDate = getLastRebaseDate(previousLevel);

				// Changes made on ancestor branches, rebased to this one
				final BoolQueryBuilder onAncestorBranch = boolQuery()
						.must(termQuery(Activity.Fields.highestPromotedBranch, ancestor))
						.must(rangeQuery(Activity.Fields.promotionDate).lt(lastRebaseDate.getTime()));
				processCommits(onAncestorBranch, componentChanges, changesNotAtTaskLevel);

				previousLevel = ancestor;
				if (isCodeSystemBranch(ancestor)) {
					// Stop at any code system level
					break;
				}
			}
		}

		if (includePromotedToThisBranch) {
			// Changes made on child branches, promoted to this one
			final BoolQueryBuilder onDescendantBranches = boolQuery()
					.mustNot(termQuery(Activity.Fields.branch, branch))
					.must(termQuery(Activity.Fields.highestPromotedBranch, branch))
					.must(rangeQuery(Activity.Fields.promotionDate).gte(activityStartDate.getTime()));
			processCommits(onDescendantBranches, componentChanges, changesNotAtTaskLevel);
		}

		if (includeMadeOnThisBranch) {
			// Changes made on this branch
			final BoolQueryBuilder onThisBranchQuery = boolQuery()
					.must(termQuery(Activity.Fields.branch, branch))
					.must(termQuery(Activity.Fields.highestPromotedBranch, branch))
					.must(rangeQuery(Activity.Fields.commitDate).gte(activityStartDate.getTime()));
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
		// TODO: if code system branch use last version date
		return new Date(0);
	}

	@Autowired
	private ActivityRepository activityRepository;

	private Date getLastRebaseDate(String branch) {
		final Page<Activity> rebases = activityRepository.findByActivityTypeAndBranch(ActivityType.REBASE, branch, MOST_RECENT_COMMIT);
		return rebases.isEmpty() ? new Date(0) : rebases.getContent().get(0).getCommitDate();
	}

	boolean isCodeSystemBranch(String branch) {
		return branch.equals("MAIN") || branch.startsWith("SNOMEDCT-", branch.lastIndexOf("/") + 1);
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
