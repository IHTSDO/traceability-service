package org.ihtsdo.otf.traceabilityservice.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

@Document(indexName = "activity")
public class Activity {

	public static class Fields {
		private Fields() {}
		public static final String highestPromotedBranch = "highestPromotedBranch";
		public static final String activityType = "activityType";
		public static final String commitDate = "commitDate";
	}

	@Id
	@Field(type = FieldType.Keyword)
	private String id;

	@Field(type = FieldType.Keyword)
	private String username;

	@Field(type = FieldType.Keyword)
	private String branch;

	@Field(type = FieldType.Keyword)
	private String sourceBranch;

	@Field(type = FieldType.Keyword)
	private String highestPromotedBranch;

	private Date commitDate;

	@Field(type = FieldType.Keyword)
	private ActivityType activityType;

	private Set<ConceptChange> conceptChanges;

	public Activity() {
	}

	public Activity(String username, String branchPath, String sourceBranch, Date commitTimestamp, ActivityType activityType) {
		this.username = username;
		this.branch = branchPath;
		this.sourceBranch = sourceBranch;
		this.commitDate = commitTimestamp;
		this.conceptChanges = new HashSet<>();
		this.activityType = activityType;
		this.highestPromotedBranch = branchPath;
	}

	public void addConceptChange(ConceptChange conceptChange) {
		conceptChanges.add(conceptChange);
	}

	public String getUsername() {
		return username;
	}

	public String getBranch() {
		return branch;
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

	public String getHighestPromotedBranch() {
		return highestPromotedBranch;
	}

	public void setHighestPromotedBranch(String highestPromotedBranch) {
		this.highestPromotedBranch = highestPromotedBranch;
	}
}
