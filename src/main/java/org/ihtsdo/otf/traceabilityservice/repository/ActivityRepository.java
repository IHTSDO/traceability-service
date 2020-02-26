package org.ihtsdo.otf.traceabilityservice.repository;

import java.util.List;
import java.util.Set;

import org.ihtsdo.otf.traceabilityservice.domain.Activity;
import org.ihtsdo.otf.traceabilityservice.domain.ActivityType;
import org.ihtsdo.otf.traceabilityservice.domain.Branch;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;

public interface ActivityRepository extends PagingAndSortingRepository<Activity, Long> {

	@Query("select a " +
			"from Activity a " +
			"where a.activityType = ?1")
	Page<Activity> findAll(ActivityType activityType, Pageable pageRequest);

	@Query("select a " +
			"from Activity a " +
			"where a.branch = ?1 ")
	Page<Activity> findByOriginalBranch(Branch branch, Pageable page);

	@Query("select a " +
			"from Activity a " +
			"where a.branch = ?1 " +
			"and a.activityType = ?2")
	Page<Activity> findByOriginalBranch(Branch branch, ActivityType activityType, Pageable page);

	@Query("select a " +
			"from Activity a " +
			"where a.branch = ?1 " +
			"or a.highestPromotedBranch = ?1")
	Page<Activity> findOnBranch(Branch branch, Pageable pageRequest);

	@Query("select a " +
			"from Activity a " +
			"where (a.branch = ?1 or a.highestPromotedBranch = ?1) " +
			"and a.activityType = ?2")
	Page<Activity> findOnBranch(Branch branch, ActivityType activityType, Pageable pageRequest);

	List<Activity> findByHighestPromotedBranchOrderByCommitDate(Branch branch);

	@Query("select a " +
			"from Activity a " +
			"join a.conceptChanges changes " +
			"where changes.conceptId = ?1")
	Page<Activity> findByConceptId(Long conceptId, Pageable pageRequest);

	@Query("select a " +
			"from Activity a " +
			"where a.activityType = ?1 " +
			"and a.commitComment like CONCAT('%',?2,'%')")
	Page<Activity> findByActivityAndComment(ActivityType activityType, String comment, Pageable page);

	@Query("select a " +
			"from Activity a " +
			"join a.conceptChanges changes " +
			"where changes.conceptId IN (?1) " +
			"and a.activityType = ?2 " +
			"and a.commitComment like CONCAT('%',?3,'%')")
	Page<Activity> findByConceptIdActivityAndCommentBulk(List<Long> conceptIds, ActivityType activityType, String comment, Pageable page);

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
	
	@Query("select a from Activity a " + 
			"where a.branch in ?1 and a.commitDate = (select max(a1.commitDate) from Activity a1 where a1.branch = a.branch ) " + 
			"      and not exists (select a2 from Activity a2 where a2.branch = a.branch and a2.commitDate > a.commitDate)")
	List<Activity> findByLastActivityOnBranches(Set<Branch> branches);
}
