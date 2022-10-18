package org.ihtsdo.otf.traceabilityservice.rest;

import com.fasterxml.jackson.databind.util.StdDateFormat;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.ihtsdo.otf.traceabilityservice.domain.Activity;
import org.ihtsdo.otf.traceabilityservice.domain.ActivityType;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.ihtsdo.otf.traceabilityservice.service.ActivityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.text.ParseException;
import java.util.*;

@RestController
@RequestMapping(produces = MediaType.APPLICATION_JSON_VALUE)
public class ActivityController {

	@Autowired
	private ActivityRepository activityRepository;

	@Autowired
	private ActivityService activityService;

	public static final Sort COMMIT_DATE_SORT = Sort.by("commitDate").ascending();
	private static final Logger LOGGER = LoggerFactory.getLogger(ActivityController.class);

	@GetMapping(value = "/activities")
	@ResponseBody
	@ApiOperation(value = "Fetch activities.", notes = "Fetch authoring activities by 'originalBranch' (the branch the activity originated on), " +
			"'onBranch' (the original branch or highest branch the activity has been promoted to). " +
			"Filtering by activity type and sorting is also available.\n" +
			"The 'brief' flag will return activities and concept changes but no component changes.\n" +
			"Note that promotions are recorded against the branch receiving the content.")
	public Page<Activity> getActivities(
			@RequestParam(required = false) @ApiParam("Find commits by the branch they were originally written to.") String originalBranch,
			@RequestParam(required = false) @ApiParam("Find commits by the original branch or highest promoted branch.") String onBranch,
			@RequestParam(required = false) @ApiParam("Find rebase or promotion commits using the source branch.") String sourceBranch,
			@RequestParam(required = false) @ApiParam("Find commits originally made on any branch starting with this prefix.") String branchPrefix,
			@RequestParam(required = false) ActivityType activityType,
			@RequestParam(required = false) @ApiParam("Find commits that changed a specific concept.") Long conceptId,
			@RequestParam(required = false) @ApiParam("Find commits that changed a specific component.") String componentId,
			@RequestParam(required = false) @ApiParam("Find commits by commit date. The format returned by the API can be used or epoch milliseconds.") String commitDate,
			@RequestParam(required = false) @ApiParam("Find commits after specified date. The format returned by the API can be used or epoch milliseconds.") String commitFromDate,
			@RequestParam(required = false) @ApiParam("Find commits before specified date. The format returned by the API can be used or epoch milliseconds.") String commitToDate,
			@RequestParam(required = false, defaultValue = "false") @ApiParam("Ignore changes made on non-International CodeSystems") boolean intOnly,
			@RequestParam(required = false, defaultValue = "false") @ApiParam("Brief response without the concept changes.") boolean brief,
			@RequestParam(required = false, defaultValue = "false") @ApiParam("Briefest response without any concept details") boolean summaryOnly,
			Pageable page) {

		if (brief || summaryOnly) {
			page = setPageDefaults(page, 1000);
		} else {
			page = setPageDefaults(page, 100);
		}

		Date commitDateDate = getDate(commitDate);
		Date commitFromDateDate = getDate(commitFromDate);
		Date commitToDateDate = getDate(commitToDate);

		ActivitySearchRequest searchRequest = new ActivitySearchRequest();
		searchRequest.setOriginalBranch(originalBranch);
		searchRequest.setOnBranch(onBranch);
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
	@ResponseBody
	@ApiOperation(value = "Fetch a filtered set of brief activities in bulk.")
	public Page<Activity> getActivitiesBulk(
			@RequestParam(required = false) ActivityType activityType,
			@RequestParam(required = false) String user,
			@RequestParam(required = false, defaultValue = "false") Boolean summary,
			@RequestBody List<Long> conceptIds,
			Pageable page) {
		LOGGER.info("Finding " + activityType + " activities for " + conceptIds.size() + " concepts.");
		page = setPageDefaults(page, 1000);
		Page<Activity> activities;
		if (summary && conceptIds.size() <= ActivityService.MAX_SIZE_FOR_PER_CONCEPT_RETRIEVAL) {
			if (user != null || activityType == null) {
				throw new IllegalArgumentException("Summaries must be run for a given activityType and cannot be filtered by user");
			}
			activities = activityService.findSummaryBy(conceptIds, activityType, page);
		} else {
			activities = activityService.findBriefInfoOnlyBy(conceptIds, activityType, user, page);
		}
		return activities;
	}

	@GetMapping(value="/activities/promotions")
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
	@ApiOperation(value = "Fetch the latest activity on multiple branches.")
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
