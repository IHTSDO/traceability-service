package org.ihtsdo.otf.traceabilityservice.migration;

import com.google.common.collect.Sets;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.ihtsdo.otf.traceabilityservice.domain.Activity;
import org.ihtsdo.otf.traceabilityservice.domain.ActivityType;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.ihtsdo.otf.traceabilityservice.service.BranchUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.stereotype.Service;

import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.*;

@Service
public class V3Point1MigrationTool {

	private static final HashSet<ActivityType> CONTENT_CHANGE_OR_CLASSIFICATION = Sets.newHashSet(ActivityType.CONTENT_CHANGE, ActivityType.CLASSIFICATION_SAVE);

	@Autowired
	private ElasticsearchOperations elasticsearchOperations;

	@Autowired
	private ActivityRepository activityRepository;

	private final Logger logger = LoggerFactory.getLogger(getClass());

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

		final Date now = new Date();
		final long day = 1000L * 60L * 60L * 24L;
		final long searchBackDays = 90L;

		for (Map.Entry<String, Date> codeSystemLastVersionDate : codeSystemBranchLastVersionDate.entrySet()) {
			final String branch = codeSystemLastVersionDate.getKey();
			Date versioningDate = codeSystemLastVersionDate.getValue();

			Date searchBackStartDate = new Date(versioningDate.getTime() - (day * searchBackDays) - 1_000);

			// Bulk lookup of all promotion dates under this code system for this date range
			Map<String, List<Date>> branchPromotionDates = getBranchPromotionDates(branch, searchBackStartDate);

			List<Activity> activitiesToUpdate = new ArrayList<>();
			logger.info("Populating promotion dates for commits in code system {} since last version ({} minus {} days = {})...", branch, versioningDate, searchBackDays, searchBackStartDate);
			while (searchBackStartDate.before(now)) {

				// Find content changes made since the last version was created.
				// For each lookup when the content was promoted and set the 'promotionDate' field.
				final BoolQueryBuilder query = boolQuery()
						.must(termsQuery(Activity.Fields.activityType, CONTENT_CHANGE_OR_CLASSIFICATION))
						.must(prefixQuery(Activity.Fields.branch, branch))
						.must(rangeQuery(Activity.Fields.commitDate).gt(searchBackStartDate.getTime()).lte(searchBackStartDate.getTime() + day));
				// Grab one day of commits at a time to keep pages below 10K max size.

				if (branch.equals("MAIN")) {
					query.mustNot(wildcardQuery(Activity.Fields.branch, "*SNOMEDCT-*"));
				}

				SearchHits<Activity> activities = elasticsearchOperations.search(new NativeSearchQueryBuilder().withQuery(query).withPageable(PageRequest.of(0, 10_000)).build(),
						Activity.class);
				for (SearchHit<Activity> hit : activities) {
					final Activity activity = hit.getContent();

					// Find date promoted to "highest promoted branch"
					Date promotionDate = searchBackStartDate;
					String highestPromotedBranch = activity.getBranch();
					while (!BranchUtil.isCodeSystemBranch(highestPromotedBranch)) {
						Date nextPromotionDate = getPromotionDate(highestPromotedBranch, promotionDate, branchPromotionDates);
						if (nextPromotionDate == null) {
							break;
						}
						promotionDate = nextPromotionDate;
						highestPromotedBranch = getParentBranch(highestPromotedBranch);
					}

					// If either highestPromotedBranch or promotionDate have changed, set them and save the updated activity
					if ((!highestPromotedBranch.equals(activity.getBranch()) && !highestPromotedBranch.equals(activity.getHighestPromotedBranch())) ||
							(promotionDate != searchBackStartDate && !promotionDate.equals(activity.getPromotionDate()))) {
						activity.setPromotionDate(promotionDate);
						activity.setHighestPromotedBranch(highestPromotedBranch);
						activitiesToUpdate.add(activity);
						System.out.print(".");
					}

					if (activitiesToUpdate.size() == 100) {
						System.out.println();
						logger.info("Updating {} commits in code system {}", activitiesToUpdate.size(), branch);
						activityRepository.saveAll(activitiesToUpdate);
						activitiesToUpdate.clear();
					}
				}
				if (!activitiesToUpdate.isEmpty()) {
					System.out.println();
					logger.info("Updating {} commits in code system {}", activitiesToUpdate.size(), branch);
					activityRepository.saveAll(activitiesToUpdate);
					activitiesToUpdate.clear();
				}
				searchBackStartDate = new Date(searchBackStartDate.getTime() + day);
			}
		}
		logger.info("Populating promotion dates completed.");
	}

	private Map<String, List<Date>> getBranchPromotionDates(String branch, Date searchBackStartDate) {
		final BoolQueryBuilder query = boolQuery()
				.must(prefixQuery(Activity.Fields.sourceBranch, branch))
				.must(termQuery(Activity.Fields.activityType, ActivityType.PROMOTION))
				.must(rangeQuery(Activity.Fields.commitDate).gt(searchBackStartDate.getTime()));

		if (branch.equals("MAIN")) {
			query.mustNot(wildcardQuery(Activity.Fields.branch, "*SNOMEDCT-*"));
		}

		Map<String, List<Date>> branchPromotionDates = new HashMap<>();
		final SearchHits<Activity> hits = elasticsearchOperations.search(new NativeSearchQueryBuilder()
				.withQuery(query)
				.withPageable(PageRequest.of(0, 10_000, Sort.by(Activity.Fields.commitDate)))
				.build(), Activity.class);
		hits.forEach(hit -> {
			final Activity promotion = hit.getContent();
			branchPromotionDates.computeIfAbsent(promotion.getSourceBranch(), b -> new ArrayList<>()).add(promotion.getCommitDate());
		});
		return branchPromotionDates;
	}

	private Date getPromotionDate(String branch, Date after, Map<String, List<Date>> branchPromotionDates) {
		return branchPromotionDates.getOrDefault(branch, Collections.emptyList()).stream().filter(date -> date.after(after)).findFirst().orElse(null);
	}

	private String getParentBranch(String branch) {
		return branch.substring(0, branch.lastIndexOf("/"));
	}

}
