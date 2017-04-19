package org.ihtsdo.otf.traceabilityservice.rest;

import org.ihtsdo.otf.traceabilityservice.domain.Activity;
import org.ihtsdo.otf.traceabilityservice.domain.ActivityType;
import org.ihtsdo.otf.traceabilityservice.domain.Branch;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.ihtsdo.otf.traceabilityservice.repository.BranchRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/activities", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
public class ActivityController {

	@Autowired
	private ActivityRepository activityRepository;

	@Autowired
	private BranchRepository branchRepository;

	@RequestMapping
	@ResponseBody
	public Page<Activity> getActivities(
			@RequestParam(required = false) String onBranch,
			@RequestParam(required = false) ActivityType activityType,
			@RequestParam(required = false) Long conceptId,
			@RequestParam(required = false, defaultValue = "0") int page,
			@RequestParam(required = false, defaultValue = "1000") int size) {

		Branch branch = null;
		if (onBranch != null) {
			branch = branchRepository.findByBranchPath(onBranch);
			if (branch == null) {
				throw new BranchNotFoundException();
			}
		}

		if (conceptId != null) {
			return activityRepository.findByConceptId(conceptId, new PageRequest(page, size));
		} else if (branch != null) {
			if (activityType != null) {
				return activityRepository.findOnBranch(branch, activityType, new PageRequest(page, size));
			} else {
				return activityRepository.findOnBranch(branch, new PageRequest(page, size));
			}
		} else {
			if (activityType != null) {
				return activityRepository.findAll(activityType, new PageRequest(page, size));
			} else {
				return activityRepository.findAll(new PageRequest(page, size));
			}
		}
	}

	@RequestMapping("/{activityId}")
	public Activity getActivity(@PathVariable String activityId) {
		return activityRepository.findOne(ControllerHelper.parseLong(activityId));
	}

}
