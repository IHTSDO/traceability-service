package org.ihtsdo.otf.traceabilityservice.migration;

import java.util.Date;
import java.util.List;

public class V2Activity {

	private Integer id;
	private User user;
	private String activityType;
	private Branch branch;
	private Branch mergeSourceBranch;
	private Branch highestPromotedBranch;
	private Date commitDate;
	private List<V2ConceptChange> conceptChanges;

	public Integer getId() {
		return id;
	}

	public User getUser() {
		return user;
	}

	public String getActivityType() {
		return activityType;
	}

	public Branch getBranch() {
		return branch;
	}

	public Branch getMergeSourceBranch() {
		return mergeSourceBranch;
	}

	public Branch getHighestPromotedBranch() {
		return highestPromotedBranch;
	}

	public Date getCommitDate() {
		return commitDate;
	}

	public List<V2ConceptChange> getConceptChanges() {
		return conceptChanges;
	}

	public static final class User {
		private String username;

		public String getUsername() {
			return username;
		}
	}

	public static final class Branch {
		private String branchPath;

		public String getBranchPath() {
			return branchPath;
		}
	}

	public static final class V2ConceptChange {
		private String conceptId;
		private List<V2ComponentChange> componentChanges;

		public String getConceptId() {
			return conceptId;
		}

		public List<V2ComponentChange> getComponentChanges() {
			return componentChanges;
		}
	}

	public static final class V2ComponentChange {
		private String componentId;
		private String changeType;
		private String componentType;
		private String componentSubType;

		public String getComponentId() {
			return componentId;
		}

		public String getChangeType() {
			return changeType;
		}

		public String getComponentType() {
			return componentType;
		}

		public String getComponentSubType() {
			return componentSubType;
		}
	}

}
