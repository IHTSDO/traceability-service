package org.ihtsdo.otf.traceabilityservice.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.util.*;

@Document(indexName = "activity")
public class Activity {

	public static class Fields {
		private Fields() {}
		public static final String username = "username";
		public static final String branch = "branch";
		public static final String sourceBranch = "sourceBranch";
		public static final String highestPromotedBranch = "highestPromotedBranch";
		public static final String commitDate = "commitDate";
		public static final String promotionDate = "promotionDate";
		public static final String activityType = "activityType";
		public static final String conceptChanges = "conceptChanges";
		public static final String conceptChangesConceptId = "conceptChanges.conceptId";

		public static final String conceptChangesComponentChanges = "conceptChanges.componentChanges";
		public static final String componentChangesComponentId = "conceptChanges.componentChanges.componentId";
		public static final String componentChangesSuperseded = "conceptChanges.componentChanges.superseded";
	}

	@Id
	@Field(type = FieldType.Keyword)
	private String id;

	@Field(type = FieldType.Keyword)
	private String username;

	@Field(type = FieldType.Keyword)
	private String branch;

	@Field(type = FieldType.Integer)
	private int branchDepth;

	@Field(type = FieldType.Keyword)
	private String sourceBranch;

	@Field(type = FieldType.Keyword)
	private String highestPromotedBranch;

	@Field(type = FieldType.Long)
	private Date commitDate;

	@Field(type = FieldType.Long)
	private Date promotionDate;

	@Field(type = FieldType.Keyword)
	private ActivityType activityType;

	@Field(type = FieldType.Object)
	private Set<ConceptChange> conceptChanges;

	public Activity() {
		this.conceptChanges = new HashSet<>();
	}

	public Activity(String username, String branchPath, String sourceBranch, Date commitTimestamp, ActivityType activityType) {
		this();
		this.username = username;
		this.branch = branchPath;
		this.branchDepth = getBranchDepth(branchPath);
		this.sourceBranch = sourceBranch;
		this.commitDate = commitTimestamp;
		this.activityType = activityType;
		this.highestPromotedBranch = branchPath;
		this.promotionDate = this.commitDate;
	}

	// Get branch depth relative to code system, relies on "SNOMEDCT-XX" code system naming convention.
	static int getBranchDepth(String branchPath) {
		branchPath = branchPath.replace("MAIN", "SNOMEDCT");
		branchPath = branchPath.replaceAll(".*SNOMEDCT-?[^/]*", "");
		return branchPath.split("/").length;
	}

	public Activity addConceptChange(ConceptChange conceptChange) {
		conceptChanges.add(conceptChange);
		return this;
	}

	public String getUsername() {
		return username;
	}

	public String getBranch() {
		return branch;
	}

	public int getBranchDepth() {
		return branchDepth;
	}

	public String getSourceBranch() {
		return sourceBranch;
	}

	public Date getCommitDate() {
		return commitDate;
	}

	public ActivityType getActivityType() {
		return activityType;
	}

	public Set<ConceptChange> getConceptChanges() {
		return conceptChanges;
	}

	public Activity setConceptChanges(Set<ConceptChange> conceptChanges) {
		this.conceptChanges = conceptChanges;
		return this;
	}

	public String getHighestPromotedBranch() {
		return highestPromotedBranch;
	}

	public void setHighestPromotedBranch(String highestPromotedBranch) {
		this.highestPromotedBranch = highestPromotedBranch;
	}

	public Date getPromotionDate() {
		return promotionDate;
	}

	public void setPromotionDate(Date promotionDate) {
		this.promotionDate = promotionDate;
	}


	public String getId() {
		return this.id;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Activity activity = (Activity) o;
		return branch.equals(activity.branch) && commitDate.equals(activity.commitDate);
	}

	@Override
	public int hashCode() {
		return Objects.hash(branch, commitDate);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", Activity.class.getSimpleName() + "[", "]").add("id='" + id + "'")
				.add("username='" + username + "'").add("branch='" + branch + "'")
				.add("sourceBranch='" + sourceBranch + "'").add("highestPromotedBranch='" + highestPromotedBranch + "'")
				.add("commitDate=" + (commitDate == null ? null : commitDate.getTime())).add("promotionDate=" + (promotionDate == null ? null: promotionDate.getTime()))
				.add("activityType=" + activityType)
				.add("total conceptChanges=" + conceptChanges.size()).toString();
	}
}


