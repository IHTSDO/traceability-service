package org.ihtsdo.otf.traceabilityservice.rest;

import io.swagger.annotations.*;
import org.ihtsdo.otf.traceabilityservice.domain.Activity;
import org.ihtsdo.otf.traceabilityservice.domain.ActivityType;
import org.ihtsdo.otf.traceabilityservice.domain.Branch;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.ihtsdo.otf.traceabilityservice.repository.BranchRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/activities", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
public class ActivityController {

	@Autowired
	private ActivityRepository activityRepository;

	@Autowired
	private BranchRepository branchRepository;

	private static final Sort COMMIT_DATE_SORT = new Sort("commitDate");

	@RequestMapping
	@ResponseBody
	@ApiOperation(value = "Fetch activities.", notes = "Fetch authoring activities by 'originalBranch' (the branch the activity originated on), " +
			"'onBranch' (the original branch or highest branch the activity has been promoted to). " +
			"Filtering by activity type and sorting is also available.")
	public Page<Activity> getActivities(
			@RequestParam(required = false) String originalBranch,
			@RequestParam(required = false) String onBranch,
			@RequestParam(required = false) ActivityType activityType,
			@RequestParam(required = false) Long conceptId,
			Pageable page) {

		if (page == null) {
			page = new PageRequest(0, 1000, COMMIT_DATE_SORT);
		}
		if (page.getSort() == null) {
			page = new PageRequest(page.getPageNumber(), page.getPageSize(), COMMIT_DATE_SORT);
		}

		if (conceptId != null) {
			return activityRepository.findByConceptId(conceptId, page);
		} else if (originalBranch != null) {
			if (activityType != null) {
				return activityRepository.findByOriginalBranch(getBranchOrThrow(originalBranch), activityType, page);
			} else {
				return activityRepository.findByOriginalBranch(getBranchOrThrow(originalBranch), page);
			}
		} else if (onBranch != null) {
			if (activityType != null) {
				return activityRepository.findOnBranch(getBranchOrThrow(onBranch), activityType, page);
			} else {
				return activityRepository.findOnBranch(getBranchOrThrow(onBranch), page);
			}
		} else {
			if (activityType != null) {
				return activityRepository.findAll(activityType, page);
			} else {
				return activityRepository.findAll(page);
			}
		}
	}

	private Branch getBranchOrThrow(@RequestParam(required = false) String branch) {
		Branch branchB = null;
		if (branch != null) {
			branchB = branchRepository.findByBranchPath(branch);
			if (branchB == null) {
				throw new BranchNotFoundException();
			}
		}
		return branchB;
	}

	@RequestMapping("/{activityId}")
	public Activity getActivity(@PathVariable String activityId) {
		return activityRepository.findOne(ControllerHelper.parseLong(activityId));
	}

}
