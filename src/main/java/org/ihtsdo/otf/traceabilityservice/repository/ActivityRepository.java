package org.ihtsdo.otf.traceabilityservice.repository;

import org.ihtsdo.otf.traceabilityservice.domain.Activity;
import org.ihtsdo.otf.traceabilityservice.domain.Branch;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;

import java.util.List;

public interface ActivityRepository extends CrudRepository<Activity, Long> {

	List<Activity> findByBranch(String branchPath);

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
