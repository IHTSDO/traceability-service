package org.ihtsdo.otf.traceabilityservice.migration;

import com.google.common.collect.Sets;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.ihtsdo.otf.traceabilityservice.domain.Activity;
import org.ihtsdo.otf.traceabilityservice.domain.ActivityType;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
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
		final long searchBackDays = 60L;

		for (Map.Entry<String, Date> codeSystemLastVersionDate : codeSystemBranchLastVersionDate.entrySet()) {
			final String branch = codeSystemLastVersionDate.getKey();
			Date versioningDate = codeSystemLastVersionDate.getValue();

			Date searchBackStartDate = new Date(versioningDate.getTime() - (day * searchBackDays) - 1_000);// minus an extra second so that today's commits are migrated.

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
				String lastBranch = null;
				Date lastBranchPromotion = null;
				for (SearchHit<Activity> hit : activities) {
					final Activity activity = hit.getContent();
					if (!activity.getBranch().equals(activity.getHighestPromotedBranch())) {

						if (lastBranch != null && lastBranch.contains("-") && lastBranch.equals(activity.getBranch())) {
							activity.setPromotionDate(lastBranchPromotion);
							activitiesToUpdate.add(activity);
						} else {
							// Find date promoted to "highest promoted branch"
							Date promotionDate = activity.getCommitDate();
							String targetBranch = activity.getBranch();
							do {
								promotionDate = getPromotionDate(targetBranch, promotionDate);
								targetBranch = getParentBranch(targetBranch);
							} while (promotionDate != null && !targetBranch.equals(activity.getHighestPromotedBranch()));

							activity.setPromotionDate(promotionDate);
							activitiesToUpdate.add(activity);

							lastBranch = activity.getBranch();
							lastBranchPromotion = promotionDate;
						}

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

	private Date getPromotionDate(String branch, Date after) {
		final BoolQueryBuilder query = boolQuery()
				.must(termQuery(Activity.Fields.sourceBranch, branch))
				.must(termQuery(Activity.Fields.activityType, ActivityType.PROMOTION))
				.must(rangeQuery(Activity.Fields.commitDate).gt(after.getTime()));

		final SearchHits<Activity> hits = elasticsearchOperations.search(new NativeSearchQueryBuilder()
				.withQuery(query)
				.withPageable(PageRequest.of(0, 1, Sort.by(Activity.Fields.commitDate)))
				.build(), Activity.class);
		if (hits.isEmpty()) {
			return null;
		} else {
			return hits.getSearchHit(0).getContent().getCommitDate();
		}
	}

	private String getParentBranch(String branch) {
		return branch.substring(0, branch.lastIndexOf("/"));
	}

}
