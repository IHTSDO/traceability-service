package org.ihtsdo.otf.traceabilityservice.domain;

import java.util.*;

public class ActivityMessage {

	private String userId;
	private String branchPath;
	private String sourceBranch;
	private ActivityType activityType;
	private Long commitTimestamp;
	private List<ConceptActivity> changes;

	public ActivityMessage() {
	}

	public String getUserId() {
		return userId;
	}

	public String getBranchPath() {
		return branchPath;
	}

	public String getSourceBranch() {
		return sourceBranch;
	}

	public ActivityType getActivityType() {
		return activityType;
	}

	public Long getCommitTimestamp() {
		return commitTimestamp;
	}

	public List<ConceptActivity> getChanges() {
		return changes;
	}

	@Override
	public String toString() {
		return "Activity{" +
				"userId='" + userId + '\'' +
				", branchPath='" + branchPath + '\'' +
				", commitTimestamp=" + commitTimestamp +
				", changes=" + changes +
				'}';
	}

	public static final class ConceptActivity {

		private String conceptId;
		private Set<ComponentChange> componentChanges;

		public ConceptActivity() {
		}

		public ConceptActivity(String conceptId) {
			this.conceptId = conceptId;
			componentChanges = new HashSet<>();
		}

		public String getConceptId() {
			return conceptId;
		}

		public Set<ComponentChange> getComponentChanges() {
			return componentChanges;
		}

		@Override
		public String toString() {
			return "ConceptActivity{" +
					"conceptId='" + conceptId + '\'' +
					", changes=" + componentChanges +
					'}';
		}
	}

	public static final class ComponentChange {

		private ChangeType changeType;
		private ComponentType componentType;
		private String componentSubType;
		private String componentId;
		private boolean effectiveTimeNull;

		public ComponentChange() {
		}

		public String getComponentId() {
			return componentId;
		}

		public ComponentType getComponentType() {
			return componentType;
		}

		public String getComponentSubType() {
			return componentSubType;
		}

		public ChangeType getChangeType() {
			return changeType;
		}

		public boolean isEffectiveTimeNull() {
			return effectiveTimeNull;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			ComponentChange that = (ComponentChange) o;
			return componentId.equals(that.componentId);
		}

		@Override
		public int hashCode() {
			return Objects.hash(componentId);
		}
	}

}
