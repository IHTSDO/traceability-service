package org.ihtsdo.otf.traceabilityservice.migration;

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import com.google.common.collect.Lists;
import org.ihtsdo.otf.traceabilityservice.util.QueryHelper;
import org.ihtsdo.otf.traceabilityservice.domain.Activity;
import org.ihtsdo.otf.traceabilityservice.domain.ActivityType;
import org.ihtsdo.otf.traceabilityservice.service.BranchUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.client.elc.NativeQueryBuilder;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.stereotype.Service;

import java.util.*;

import static co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders.bool;

/**
 * This is to apply data fixes caused by FRI-305.
 * Issues: Rebase activities with content changes are not promoted as shown below:
 * e.g Content changes for ICD1120-420 task have been promoted to project ICD1120 and MAIN however
 * the rebase activity below is not and the highestPromotedBranch is still pointing to itself.
 * Before fix:
 * "branch" : "MAIN/ICD1120/ICD1120-420",
 * "branchDepth" : 3,
 * "sourceBranch" : "MAIN/ICD1120",
 * "highestPromotedBranch" : "MAIN/ICD1120/ICD1120-420",
 * "commitDate" : 1635518961063,
 * "activityType" : "REBASE"
 * <p>
 *
 * After fix:
 * "branch" : "MAIN/ICD1120/ICD1120-420",
 * "branchDepth" : 3,
 * "sourceBranch" : "MAIN/ICD1120",
 * "highestPromotedBranch" : "MAIN",
 * "commitDate" : 1635518961063,
 * "promotionDate" : 1635754059687,
 * "activityType" : "REBASE"
 *
 */

@Service
public class V3Point2MigrationTool extends V3Point1MigrationTool {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Value("${migration.save-batch-size}")
	private int saveBatchSize;
	private final boolean dryRun = true;

	@Override
	public void start() {
		final List<Activity> codeSystemVersions = activityRepository.findByActivityType(ActivityType.CREATE_CODE_SYSTEM_VERSION, Pageable.unpaged()).getContent();

		if (codeSystemVersions.isEmpty()) {
			throw new IllegalStateException("There are no code system version events in the store. " +
					"Please run the Snowstorm function 'POST /admin/actions/traceability-backfill' first.");
		}

		// Collect code system branches and the date that the content was versioned (not the effectiveTime).
		Map<String, Date> codeSystemBranchLastVersionDate = new HashMap<>();
		for (Activity codeSystemVersion : codeSystemVersions) {
			final String branch = codeSystemVersion.getBranch();
			if (codeSystemVersion.getCommitDate().after(codeSystemBranchLastVersionDate.getOrDefault(branch, new Date(0)))) {
				codeSystemBranchLastVersionDate.put(branch, codeSystemVersion.getCommitDate());
			}
		}

		logger.info("{} code system version activities found", codeSystemBranchLastVersionDate.keySet().size());

		for (Map.Entry<String, Date> codeSystemLastVersionDate : codeSystemBranchLastVersionDate.entrySet()) {
			final String branch = codeSystemLastVersionDate.getKey();

			logger.info("Start processing rebase activities promotion on {} ...", branch);
			Date versioningDate = codeSystemLastVersionDate.getValue();

			// Check any rebase activities have not been promoted since last versioning
			List<Activity> rebaseActivities = getRebaseActivitiesNotPromoted(branch, versioningDate);
			logger.info("{} rebase activities with content changes on {} since last versioning haven't been promoted", rebaseActivities.size(), branch);
			if (rebaseActivities.isEmpty()) {
				continue;
			}

			// Bulk lookup of all promotion dates under this code system after last versioning
			Map<String, List<Date>> branchPromotionDates = getBranchPromotionDates(branch, versioningDate);
			logger.info("{} branches with promotions since last versioning {} on {}", branchPromotionDates.keySet().size(), versioningDate, branch);

			List<Activity> activitiesToUpdate = new ArrayList<>();
			for (Activity activity : rebaseActivities) {
				// Skip update if the rebase activity has no content change and no promotions on that branch
				if (activity.getConceptChanges().isEmpty() || !branchPromotionDates.containsKey(activity.getHighestPromotedBranch())) {
					if (dryRun) {
						logger.info("Rebase skipped as no promotion made on branch yet {}", activity);
					}
					continue;
				}
				// Find date promoted to "highest promoted branch" after the rebase commit date
				// Before data fix the promotionDate on rebase is not set so use commit date
				Date promotionDate = activity.getCommitDate();
				String highestPromotedBranch = activity.getBranch();
				while (!BranchUtils.isCodeSystemBranch(highestPromotedBranch)) {
					Date nextPromotionDate = getPromotionDate(highestPromotedBranch, promotionDate, branchPromotionDates);
					if (nextPromotionDate == null) {
						break;
					}
					promotionDate = nextPromotionDate;
					highestPromotedBranch = getParentBranch(highestPromotedBranch);
				}

				// If highestPromotedBranch is changed and promotionDate is after the rebase commitDate
				// then the rebase activity should have been promoted and needs to be updated
				if (!highestPromotedBranch.equals(activity.getHighestPromotedBranch()) && promotionDate.after(activity.getCommitDate())) {
					logger.info("Before: {}", activity);
					activity.setPromotionDate(promotionDate);
					activity.setHighestPromotedBranch(highestPromotedBranch);
					activitiesToUpdate.add(activity);
					logger.info("After: {}", activity);
				} else {
					if (dryRun) {
						logger.info("Rebase skipped as no promotion made on branch after {}", activity);
					}
				}
			}
			logger.info("{} rebase commits to be updated in code system {}", activitiesToUpdate.size(), branch);

			// Batch update
			if (!dryRun) {
				Lists.partition(activitiesToUpdate, saveBatchSize).forEach(job -> {
					activityRepository.saveAll(job);
					logger.info("Updated {} rebase commits in code system {}", activitiesToUpdate.size(), branch);
				});
			}
			logger.info("Rebase activities promotion completed on {}", branch);
		}
		logger.info("Rebase activities promotions are completed for all code system branches");
	}

	private List<Activity> getRebaseActivitiesNotPromoted(String codeSystemPath, Date lastVersion) {
		RangeQuery.Builder rangeQueryBuilder = QueryHelper.rangeQueryBuilder(Activity.Fields.COMMIT_DATE);
		QueryHelper.withFrom(rangeQueryBuilder, lastVersion.getTime());
		BoolQuery.Builder query = bool()
				.must(QueryHelper.prefixQuery(Activity.Fields.BRANCH, codeSystemPath))
				.must(QueryHelper.termQuery(Activity.Fields.ACTIVITY_TYPE, ActivityType.REBASE.name()))
				.must(QueryHelper.toQuery(rangeQueryBuilder))
				.must(QueryHelper.existsQuery(Activity.Fields.HIGHEST_PROMOTED_BRANCH))
				.must(QueryHelper.existsQuery("conceptChanges"))
				.mustNot(QueryHelper.existsQuery(Activity.Fields.PROMOTION_DATE));

		// Exclude extensions when checking on MAIN for International
		if (codeSystemPath.equals("MAIN")) {
			query.mustNot(QueryHelper.wildcardQuery(Activity.Fields.BRANCH, "*SNOMEDCT-*"));
		}

		SearchHits<Activity> searchHits = elasticsearchOperations.search(new NativeQueryBuilder().withQuery(QueryHelper.toQuery(query)).withPageable(PageRequest.of(0, 10_000)).build(), Activity.class);
		if (searchHits.getTotalHits() > 10_000) {
			logger.warn("Found over 10K rebase activities(total {}) with content changes on {} since last versioning", searchHits.getTotalHits(), codeSystemPath);
		}
		List<Activity> allRebasedActivities = new ArrayList<>();
		searchHits.forEach(hit -> allRebasedActivities.add(hit.getContent()));
		return allRebasedActivities;
	}
}

