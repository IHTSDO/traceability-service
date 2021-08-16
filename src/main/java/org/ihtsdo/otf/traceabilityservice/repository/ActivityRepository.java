package org.ihtsdo.otf.traceabilityservice.repository;

import org.ihtsdo.otf.traceabilityservice.domain.Activity;
import org.ihtsdo.otf.traceabilityservice.domain.ActivityType;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.repository.PagingAndSortingRepository;

import java.util.List;

public interface ActivityRepository extends PagingAndSortingRepository<Activity, Long> {

	Page<Activity> findByActivityTypeOrderByCommitDate(ActivityType activityType, Pageable pageRequest);

	Page<Activity> findByBranchOrderByCommitDate(String branch, Pageable page);

	Page<Activity> findByBranchOrderByCommitDateDesc(String branch, Pageable page);

	Page<Activity> findByBranchAndActivityTypeOrderByCommitDate(String branch, ActivityType activityType, Pageable page);

	Page<Activity> findByHighestPromotedBranchOrderByCommitDate(String highestPromotedBranch, Pageable pageRequest);

	Page<Activity> findByActivityTypeAndHighestPromotedBranchOrderByCommitDate(ActivityType activityType, String highestPromotedBranch, Pageable pageRequest);

	@Query("{ \"term\" : { \"conceptChanges.conceptId\" : \"?0\" } }")
	Page<Activity> findByConceptIdOrderByCommitDate(Long conceptId, Pageable pageRequest);

	Page<Activity> findByActivityTypeAndSourceBranch(ActivityType activityType, String sourceBranch, Pageable page);

	@Query("{ \"bool\": { \"must\": [ " +
			"{ \"term\" : { \"activityType\" : \"?0\" } }, " +
			"{ \"term\" : { \"conceptChanges.conceptId\" : \"?1\" } }, " +
			"{ \"term\" : { \"user\" : \"?2\" } } " +
			"] } }")
	Page<Activity> findByActivityTypeConceptIdAndUserOrderByCommitDate(ActivityType activityType, List<Long> conceptIds, String user, Pageable page);

}
