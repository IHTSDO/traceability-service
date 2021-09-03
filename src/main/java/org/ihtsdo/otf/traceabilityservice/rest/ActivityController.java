package org.ihtsdo.otf.traceabilityservice.rest;

import io.swagger.annotations.ApiOperation;
import org.ihtsdo.otf.traceabilityservice.domain.Activity;
import org.ihtsdo.otf.traceabilityservice.domain.ActivityType;
import org.ihtsdo.otf.traceabilityservice.domain.ConceptChange;
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

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping(produces = MediaType.APPLICATION_JSON_VALUE)
public class ActivityController {

	@Autowired
	private ActivityRepository activityRepository;

	@Autowired
	private ActivityService activityService;

	public static final Sort COMMIT_DATE_SORT = Sort.by("commitDate").descending();
	private static final Logger LOGGER = LoggerFactory.getLogger(ActivityController.class);

	@GetMapping(value = "/activities")
	@ResponseBody
	@ApiOperation(value = "Fetch activities.", notes = "Fetch authoring activities by 'originalBranch' (the branch the activity originated on), " +
			"'onBranch' (the original branch or highest branch the activity has been promoted to). " +
			"Filtering by activity type and sorting is also available.\n" +
			"The 'brief' flag will return activities and concept changes but no component changes.\n" +
			"Note that promotions are recorded against the branch receiving the content.")
	public Page<Activity> getActivities(
			@RequestParam(required = false) String originalBranch,
			@RequestParam(required = false) String onBranch,
			@RequestParam(required = false) ActivityType activityType,
			@RequestParam(required = false) Long conceptId,
			@RequestParam(required = false, defaultValue = "false") boolean brief,
			Pageable page) {

		page = setPageDefaults(page, 1000);
		Page<Activity> activities = doGetActivities(originalBranch, onBranch, activityType, conceptId, page);
		if (brief) {
			makeBrief(activities);
		}
		return activities;
	}

	private Page<Activity> doGetActivities(String originalBranch, String onBranch, ActivityType activityType, Long conceptId, Pageable page) {
		if (conceptId != null) {
			return activityRepository.findByConceptId(conceptId, page);
		} else if (originalBranch != null) {
			if (activityType != null) {
				return activityRepository.findByBranchAndActivityType(originalBranch, activityType, page);
			} else {
				return activityRepository.findByBranch(originalBranch, page);
			}
		} else if (onBranch != null) {
			if (activityType != null) {
				return activityRepository.findByHighestPromotedBranchOrBranchAndActivityType(onBranch, onBranch, activityType, page);
			} else {
				return activityRepository.findByHighestPromotedBranchOrBranch(onBranch, onBranch, page);
			}
		} else {
			if (activityType != null) {
				return activityRepository.findByActivityType(activityType, page);
			} else {
				return activityRepository.findAll(page);
			}
		}
	}
	
	
	@PostMapping(value = "/activitiesBulk")
	@ResponseBody
	@ApiOperation(value = "Fetch a filtered set of brief activities in bulk.")
	public Page<Activity> getActivitiesBulk(
			@RequestParam(required = false) ActivityType activityType,
			@RequestParam(required = false) String user,
			@RequestBody List<Long> conceptIds,
			Pageable page) {
		LOGGER.info("Finding activities in bulk.");
		page = setPageDefaults(page, 1000);
		Page<Activity> activities = activityService.findBy(conceptIds, activityType, user, page);
		makeBrief(activities);
		return activities;
	}

	@GetMapping(value="/activities/promotions")
	public Page<Activity> getPromotions(@RequestParam String sourceBranch, Pageable page) {
		page = setPageDefaults(page, 100);
		return activityRepository.findByActivityTypeAndSourceBranch(ActivityType.PROMOTION, sourceBranch, page);
	}

	@GetMapping(value="/activities/{activityId}")
	public Activity getActivity(@PathVariable String activityId) {
		return activityRepository.findById(ControllerHelper.parseLong(activityId)).orElse(null);
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
		}
		if (page.getSort() == Sort.unsorted()) {
			page = PageRequest.of(page.getPageNumber(), page.getPageSize(), COMMIT_DATE_SORT);
		}
		return page;
	}
	
	private void makeBrief(Page<Activity> activities) {
		for (Activity activity : activities.getContent()) {
			for (ConceptChange conceptChange : activity.getConceptChanges()) {
				conceptChange.getComponentChanges().clear();
			}
		}
	}
}
