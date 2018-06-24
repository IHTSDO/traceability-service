package org.ihtsdo.otf.traceabilityservice.rest;

import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
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
	public Page<Activity> getActivities(
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

		Branch branch = null;
		if (onBranch != null) {
			branch = branchRepository.findByBranchPath(onBranch);
			if (branch == null) {
				throw new BranchNotFoundException();
			}
		}

		if (conceptId != null) {
			return activityRepository.findByConceptId(conceptId, page);
		} else if (branch != null) {
			if (activityType != null) {
				return activityRepository.findOnBranch(branch, activityType, page);
			} else {
				return activityRepository.findOnBranch(branch, page);
			}
		} else {
			if (activityType != null) {
				return activityRepository.findAll(activityType, page);
			} else {
				return activityRepository.findAll(page);
			}
		}
	}

	@RequestMapping("/{activityId}")
	public Activity getActivity(@PathVariable String activityId) {
		return activityRepository.findOne(ControllerHelper.parseLong(activityId));
	}

}
