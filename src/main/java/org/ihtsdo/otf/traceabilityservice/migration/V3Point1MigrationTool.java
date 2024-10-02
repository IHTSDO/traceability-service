package org.ihtsdo.otf.traceabilityservice.migration;

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import com.google.common.collect.Sets;
import org.ihtsdo.otf.traceabilityservice.util.QueryHelper;
import org.ihtsdo.otf.traceabilityservice.domain.Activity;
import org.ihtsdo.otf.traceabilityservice.domain.ActivityType;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.ihtsdo.otf.traceabilityservice.service.BranchUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.client.elc.NativeQueryBuilder;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.stereotype.Service;

import java.util.*;

import static co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders.bool;

@Service
public class V3Point1MigrationTool {

	private static final HashSet<ActivityType> CONTENT_CHANGE_OR_CLASSIFICATION = Sets.newHashSet(ActivityType.CONTENT_CHANGE, ActivityType.CLASSIFICATION_SAVE);

	@Autowired
	ElasticsearchOperations elasticsearchOperations;

	@Autowired
	ActivityRepository activityRepository;

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
				final BoolQuery.Builder query = bool()
						.must(QueryHelper.termsQuery(Activity.Fields.ACTIVITY_TYPE, CONTENT_CHANGE_OR_CLASSIFICATION))
						.must(QueryHelper.prefixQuery(Activity.Fields.BRANCH, branch))
						.must(QueryHelper.rangeQuery(Activity.Fields.COMMIT_DATE,searchBackStartDate.getTime(),searchBackStartDate.getTime() + day));
				// Grab one day of commits at a time to keep pages below 10K max size.

				if (branch.equals("MAIN")) {
					query.mustNot(QueryHelper.wildcardQuery(Activity.Fields.BRANCH, "*SNOMEDCT-*"));
				}

				SearchHits<Activity> activities = elasticsearchOperations.search(new NativeQueryBuilder().withQuery(QueryHelper.toQuery(query)).withPageable(PageRequest.of(0, 10_000)).build(),
						Activity.class);
				for (SearchHit<Activity> hit : activities) {
					final Activity activity = hit.getContent();

					// Find date promoted to "highest promoted branch"
					Date promotionDate = searchBackStartDate;
					String highestPromotedBranch = activity.getBranch();
					while (!BranchUtils.isCodeSystemBranch(highestPromotedBranch)) {
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

	Map<String, List<Date>> getBranchPromotionDates(String branch, Date searchBackStartDate) {
		RangeQuery.Builder rangeQueryBuilder = QueryHelper.rangeQueryBuilder(Activity.Fields.COMMIT_DATE);
		QueryHelper.withFrom(rangeQueryBuilder, searchBackStartDate.getTime());
		final BoolQuery.Builder query = bool()
				.must(QueryHelper.prefixQuery(Activity.Fields.SOURCE_BRANCH, branch))
				.must(QueryHelper.termQuery(Activity.Fields.ACTIVITY_TYPE, ActivityType.PROMOTION.name()))
				.must(QueryHelper.toQuery(rangeQueryBuilder));

		if (branch.equals("MAIN")) {
			query.mustNot(QueryHelper.wildcardQuery(Activity.Fields.BRANCH, "*SNOMEDCT-*"));
		}

		Map<String, List<Date>> branchPromotionDates = new HashMap<>();
		final SearchHits<Activity> hits = elasticsearchOperations.search(new NativeQueryBuilder()
				.withQuery(QueryHelper.toQuery(query))
				.withPageable(PageRequest.of(0, 10_000, Sort.by(Activity.Fields.COMMIT_DATE)))
				.build(), Activity.class);
		hits.forEach(hit -> {
			final Activity promotion = hit.getContent();
			branchPromotionDates.computeIfAbsent(promotion.getSourceBranch(), b -> new ArrayList<>()).add(promotion.getCommitDate());
		});
		return branchPromotionDates;
	}

	Date getPromotionDate(String branch, Date after, Map<String, List<Date>> branchPromotionDates) {
		return branchPromotionDates.getOrDefault(branch, Collections.emptyList()).stream().filter(date -> date.after(after)).findFirst().orElse(null);
	}

	String getParentBranch(String branch) {
		return branch.substring(0, branch.lastIndexOf("/"));
	}

}
