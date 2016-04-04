package org.ihtsdo.otf.traceabilityservice.rest;

import com.google.common.base.Strings;
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
			@RequestParam(required = false) String activityType,
			@RequestParam(required = false, defaultValue = "0") int page,
			@RequestParam(required = false, defaultValue = "1000") int size) {

		ActivityType activityTypeEnum = null;
		if (!Strings.isNullOrEmpty(activityType)) {
			activityTypeEnum = ActivityType.valueOf(activityType.toUpperCase());
		}

		Branch branch = null;
		if (!Strings.isNullOrEmpty(onBranch)) {
			branch = branchRepository.findByBranchPath(onBranch);
			if (branch == null) {
				throw new BranchNotFoundException();
			}
		}

		if (branch != null) {
			if (activityTypeEnum != null) {
				return activityRepository.findOnBranch(branch, activityTypeEnum, new PageRequest(page, size));
			} else {
				return activityRepository.findOnBranch(branch, new PageRequest(page, size));
			}
		} else {
			if (activityTypeEnum != null) {
				return activityRepository.findAll(activityTypeEnum, new PageRequest(page, size));
			} else {
				return activityRepository.findAll(new PageRequest(page, size));
			}
		}
	}

}
