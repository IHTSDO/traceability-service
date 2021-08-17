package org.ihtsdo.otf.traceabilityservice.repository;

import org.ihtsdo.otf.traceabilityservice.domain.Activity;
import org.ihtsdo.otf.traceabilityservice.domain.ActivityType;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.repository.PagingAndSortingRepository;

import java.util.List;

public interface ActivityRepository extends PagingAndSortingRepository<Activity, Long> {

	Page<Activity> findByActivityType(ActivityType activityType, Pageable pageRequest);

	Page<Activity> findByBranch(String branch, Pageable page);

	Page<Activity> findByBranchAndActivityType(String branch, ActivityType activityType, Pageable page);

	Page<Activity> findByHighestPromotedBranch(String highestPromotedBranch, Pageable pageRequest);

	Page<Activity> findByActivityTypeAndHighestPromotedBranch(ActivityType activityType, String highestPromotedBranch, Pageable pageRequest);

	@Query("{ \"term\" : { \"conceptChanges.conceptId\" : \"?0\" } }")
	Page<Activity> findByConceptId(Long conceptId, Pageable pageRequest);

	Page<Activity> findByActivityTypeAndSourceBranch(ActivityType activityType, String sourceBranch, Pageable page);

	@Query("{ \"bool\": { \"must\": [ " +
			"{ \"term\" : { \"activityType\" : \"?0\" } }, " +
			"{ \"term\" : { \"conceptChanges.conceptId\" : \"?1\" } }, " +
			"{ \"term\" : { \"user\" : \"?2\" } } " +
			"] } }")
	Page<Activity> findByActivityTypeConceptIdAndUser(ActivityType activityType, List<Long> conceptIds, String user, Pageable page);

}
