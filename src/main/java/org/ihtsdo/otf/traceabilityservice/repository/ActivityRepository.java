package org.ihtsdo.otf.traceabilityservice.repository;

import org.ihtsdo.otf.traceabilityservice.domain.Activity;
import org.ihtsdo.otf.traceabilityservice.domain.ActivityType;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.repository.PagingAndSortingRepository;

import java.util.List;

public interface ActivityRepository extends PagingAndSortingRepository<Activity, String> {

	Page<Activity> findByActivityType(ActivityType activityType, Pageable pageRequest);

	Page<Activity> findByBranch(String branch, Pageable page);

	Page<Activity> findByBranchAndActivityType(String branch, ActivityType activityType, Pageable page);

	Page<Activity> findByHighestPromotedBranchOrBranch(String highestPromotedBranch, String branch, Pageable pageRequest);

	@Query("{ \"bool\": { \"must\": [ " +
			"{ \"bool\" : { \"should\" : [" +
			"  { \"term\" : { \"highestPromotedBranch\" : \"?0\" } }, " +
			"  { \"term\" : { \"branch\" : \"?1\" } } " +
			"] }}, " +
			"{ \"term\" : { \"activityType\" : \"?2\" } } " +
			"] }}")
	Page<Activity> findByHighestPromotedBranchOrBranchAndActivityType(String highestPromotedBranch, String branch, ActivityType activityType, Pageable pageRequest);

	@Query("{ \"term\" : { \"conceptChanges.conceptId\" : \"?0\" } }")
	Page<Activity> findByConceptId(Long conceptId, Pageable pageRequest);

	Page<Activity> findByActivityTypeAndSourceBranch(ActivityType activityType, String sourceBranch, Pageable page);

	Page<Activity> findByActivityTypeAndBranch(ActivityType activityType, String sourceBranch, Pageable page);

	@Query("{ \"bool\": { \"must\": [ " +
			"{ \"terms\" : { \"conceptChanges.conceptId\" : ?0 } }, " +
			"{ \"term\" : { \"activityType\" : \"?1\" } }, " +
			"{ \"term\" : { \"username\" : \"?2\" } } " +
			"] } }")
	Page<Activity> findBy(List<Long> conceptIds, ActivityType activityType, String user, Pageable page);

	@Query("{ \"bool\": { \"must\": [ " +
			"{ \"terms\" : { \"conceptChanges.conceptId\" : ?0 } }, " +
			"{ \"term\" : { \"activityType\" : \"?1\" } } " +
			"] } }")
	Page<Activity> findBy(List<Long> conceptIds, ActivityType activityType, Pageable page);

	@Query("{ \"bool\": { \"must\": [ " +
			"{ \"terms\" : { \"conceptChanges.conceptId\" : ?0 } }, " +
			"{ \"term\" : { \"username\" : \"?1\" } } " +
			"] } }")
	Page<Activity> findBy(List<Long> conceptIds, String user, Pageable page);

	@Query("{ \"bool\": { \"must\": [ " +
			"{ \"terms\" : { \"conceptChanges.conceptId\" : ?0 } } " +
			"] } }")
	Page<Activity> findBy(List<Long> conceptIds, Pageable page);

}
