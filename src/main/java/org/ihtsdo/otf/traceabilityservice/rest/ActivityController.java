package org.ihtsdo.otf.traceabilityservice.rest;

import com.fasterxml.jackson.databind.util.StdDateFormat;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.ihtsdo.otf.traceabilityservice.domain.Activity;
import org.ihtsdo.otf.traceabilityservice.domain.ActivityType;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.ihtsdo.otf.traceabilityservice.service.ActivitySearchRequest;
import org.ihtsdo.otf.traceabilityservice.service.ActivityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springdoc.core.converters.models.PageableAsQueryParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.text.ParseException;
import java.util.*;

@RestController
@Tag(name = "Activity")
@RequestMapping(produces = MediaType.APPLICATION_JSON_VALUE)
public class ActivityController {
	@Value("${traceability.max.activities.page.size:500}")
	private int maxActivities;
	public static final int MAX_BULK_SIZE = 100;
	@Autowired
	private ActivityRepository activityRepository;

	@Autowired
	private ActivityService activityService;

	public static final Sort COMMIT_DATE_SORT = Sort.by("commitDate").ascending();
	private static final Logger LOGGER = LoggerFactory.getLogger(ActivityController.class);

	@GetMapping(value = "/activities")
	@PageableAsQueryParam
	@Operation(summary = "Fetch activities.",
			description = """
            Fetch authoring activities by 'originalBranch' (the branch the activity originated on), 'onBranch' (the original branch or highest branch the activity has been promoted to). Filtering by activity type and sorting is also available.
            The 'brief' flag will return activities and concept changes but no component changes.
            Note that promotions are recorded against the branch receiving the content.""")
	public Page<Activity> getActivities(
			@RequestParam(required = false) @Parameter(description = "Find commits by the branch they were originally written to.") String originalBranch,
			@RequestParam(required = false) @Parameter(description = "Find commits by the original branch or highest promoted branch.") String onBranch,
			@RequestParam(required = false, defaultValue = "false") @Parameter(description = "Include commits that have been promoted further, that may have been rebased down to specified branch") Boolean includeHigherPromotions,
			@RequestParam(required = false) @Parameter(description = "Find rebase or promotion commits using the source branch.") String sourceBranch,
			@RequestParam(required = false) @Parameter(description = "Find commits originally made on any branch starting with this prefix.") String branchPrefix,
			@RequestParam(required = false) ActivityType activityType,
			@RequestParam(required = false) @Parameter(description = "Find commits that changed a specific concept.") Long conceptId,
			@RequestParam(required = false) @Parameter(description = "Find commits that changed a specific component.") String componentId,
			@RequestParam(required = false) @Parameter(description = "Find commits by commit date. The format returned by the API can be used or epoch milliseconds.") String commitDate,
			@RequestParam(required = false) @Parameter(description = "Find commits after specified date. The format returned by the API can be used or epoch milliseconds.") String commitFromDate,
			@RequestParam(required = false) @Parameter(description = "Find commits before specified date. The format returned by the API can be used or epoch milliseconds.") String commitToDate,
			@RequestParam(required = false, defaultValue = "false") @Parameter(description = "Ignore changes made on non-International CodeSystems") boolean intOnly,
			@RequestParam(required = false, defaultValue = "false") @Parameter(description = "Brief response without the concept changes.") boolean brief,
			@RequestParam(required = false, defaultValue = "false") @Parameter(description = "Briefest response without any concept details") boolean summaryOnly,
			Pageable page) {

		if (brief || summaryOnly) {
			page = setPageDefaults(page, 1000);
		} else {
			// To prevent fetching too many documents
			if (page != null && page.getPageSize() > maxActivities) {
				throw new IllegalArgumentException(String.format("Page size of %d exceeds maximum %d", page.getPageSize(), maxActivities));
			}
			page = setPageDefaults(page, maxActivities);
		}

		Date commitDateDate = getDate(commitDate);
		Date commitFromDateDate = getDate(commitFromDate);
		Date commitToDateDate = getDate(commitToDate);

		ActivitySearchRequest searchRequest = new ActivitySearchRequest();
		searchRequest.setOriginalBranch(originalBranch);
		searchRequest.setOnBranch(onBranch);
		searchRequest.setIncludeHigherPromotions(includeHigherPromotions);
		searchRequest.setSourceBranch(sourceBranch);
		searchRequest.setBranchPrefix(branchPrefix);
		searchRequest.setActivityType(activityType);
		searchRequest.setConceptId(conceptId);
		searchRequest.setComponentId(componentId);
		searchRequest.setCommitDate(commitDateDate);
		searchRequest.setFromDate(commitFromDateDate);
		searchRequest.setToDate(commitToDateDate);
		searchRequest.setIntOnly(intOnly);
		searchRequest.setBrief(brief);
		searchRequest.setSummaryOnly(summaryOnly);
		return activityService.getActivities(searchRequest, page);
	}
	
	private Date getDate(String commitDate) {
		if (commitDate != null && !commitDate.isEmpty()) {
			if (commitDate.matches("\\d*")) {
				return new Date(Long.parseLong(commitDate));
			} else {
				final StdDateFormat df = new StdDateFormat();
				df.setTimeZone(TimeZone.getTimeZone("UTC"));
				try {
					commitDate = commitDate.replace(" ", "+");
					return df.parse(commitDate);
				} catch (ParseException e) {
					throw new IllegalArgumentException("Bad date format. Try URL encoding. " + e.getMessage() + ".", e);
				}
			}
		}
		return null;
	}

	@PostMapping(value = "/activitiesBulk")
	@Operation(summary = "Fetch a filtered set of brief activities in bulk.")
	@PageableAsQueryParam
	public Page<Activity> getActivitiesBulk(
			@RequestParam(required = false) ActivityType activityType,
			@RequestParam(required = false) String user,
			@RequestParam(required = false, defaultValue = "false") Boolean summary,
			@RequestBody List<Long> conceptIds,
			Pageable page) {
		LOGGER.info("Finding {} activities for {} concepts", activityType, conceptIds.size());
		if (conceptIds.size() > MAX_BULK_SIZE) {
			throw new IllegalArgumentException(String.format("%d concept ids exceed the maximum size of %d", conceptIds.size(), MAX_BULK_SIZE));
		}
		page = setPageDefaults(page, MAX_BULK_SIZE);
		return activityService.findActivitiesBy(conceptIds, activityType, user, summary, page);
	}

	@GetMapping(value = "/activitiesForUsersOnBranches")
	@Operation(summary = "Fetch a filtered set of authoring activities (ie activity type = ContentChange) for users on specific branches")
	@PageableAsQueryParam
	public Page<Activity> getActivitiesForUsersOnBranches(
			@RequestParam(required = false) @Parameter(description = "Set this to 733073007 to identify changes to axioms ie modeling changes") String componentSubType,
			@RequestParam(required = false) @Parameter(description = "A single, or comma separated list of users") String users,
			@RequestParam(required = false) @Parameter(description = "A single, or comma separated list of branch prefixes to check eg MAIN/SNOMEDCT-CSR/NEBCSR") String branches,
			@RequestParam(required = false)
			@Parameter(description = "Changes made on or after this date (YYYY-MM-DD)")
			@DateTimeFormat(pattern = "yyyy-MM-dd")
			Date since,

			Pageable page) {
		LOGGER.info("Finding updates to {} components by {} on {} branches (prefixed) since {}", componentSubType, users, branches, since);
		page = setPageDefaults(page, MAX_BULK_SIZE);
		return activityService.findActivitiesBy(componentSubType, users, branches, since, page);
	}

	@GetMapping(value="/activities/promotions")
	@PageableAsQueryParam
	public Page<Activity> getPromotions(@RequestParam String sourceBranch, Pageable page) {
		// Promotion doesn't log component changes so the page size 1000 is fine.
		page = setPageDefaults(page, 1000);
		return activityRepository.findByActivityTypeAndSourceBranch(ActivityType.PROMOTION, sourceBranch, page);
	}

	@GetMapping(value="/activities/{activityId}")
	public Activity getActivity(@PathVariable String activityId) {
		return activityRepository.findById(activityId).orElse(null);
	}

	@PostMapping(value = "/activities/branches/last")
	@Operation(summary = "Fetch the latest activity on multiple branches.")
	public List<Activity> getLastModifiedOnBranches (@RequestBody List<String> branches) {
		List<Activity> activities = new ArrayList<>();
		final PageRequest sortedPage = PageRequest.of(0, 1, COMMIT_DATE_SORT.descending());
		for (String branch : branches) {
			final Page<Activity> page = activityRepository.findByBranch(branch, sortedPage);
			if (!page.isEmpty()) {
				activities.add(page.getContent().get(0));
			}
		}
		return activities;
	}

	private Pageable setPageDefaults(Pageable page, int maxSize) {
		if (page == null) {
			page = PageRequest.of(0, maxSize, COMMIT_DATE_SORT);
		} else {
			page = PageRequest.of(page.getPageNumber(), Math.min(page.getPageSize(), maxSize), page.getSort());
		}
		if (page.getSort() == Sort.unsorted()) {
			page = PageRequest.of(page.getPageNumber(), page.getPageSize(), COMMIT_DATE_SORT);
		}
		return page;
	}
}
