package org.ihtsdo.otf.traceabilityservice.repository;

import org.ihtsdo.otf.traceabilityservice.domain.Activity;
import org.ihtsdo.otf.traceabilityservice.domain.ActivityType;
import org.ihtsdo.otf.traceabilityservice.domain.Branch;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;

import java.util.List;

public interface ActivityRepository extends PagingAndSortingRepository<Activity, Long> {

	@Query("select a " +
			"from Activity a " +
			"where a.activityType = ?1 " +
			"order by a.commitDate")
	Page<Activity> findAll(ActivityType activityType, Pageable pageRequest);

	@Query("select a " +
			"from Activity a " +
			"where a.branch = ?1 " +
			"or a.highestPromotedBranch = ?1 " +
			"order by a.commitDate")
	Page<Activity> findOnBranch(Branch branch, Pageable pageRequest);

	@Query("select a " +
			"from Activity a " +
			"where (a.branch = ?1 or a.highestPromotedBranch = ?1) " +
			"and a.activityType = ?2 " +
			"order by a.commitDate")
	Page<Activity> findOnBranch(Branch branch, ActivityType activityType, Pageable pageRequest);

	List<Activity> findByHighestPromotedBranchOrderByCommitDate(Branch branch);

	/**
	 * This helps us track where an Activity has been promoted to.
	 */
	@Modifying
	@Query("update Activity " +
			"set highestPromotedBranch = ?1 " +
			"where highestPromotedBranch = ?2 " +
			"and activityType <> org.ihtsdo.otf.traceabilityservice.domain.ActivityType.REBASE " +
			"and activityType <> org.ihtsdo.otf.traceabilityservice.domain.ActivityType.PROMOTION")
	void setHighestPromotedBranchWhereBranchEquals(Branch newHighestPromotedBranch, Branch oldHighestPromotedBranch);

}
