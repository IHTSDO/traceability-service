package org.ihtsdo.otf.traceabilityservice.service;

import org.ihtsdo.otf.traceabilityservice.domain.ActivityType;

import java.util.Date;
import java.util.StringJoiner;

public class ActivitySearchRequest {
    private String originalBranch;

    private String onBranch;

    private String sourceBranch;

    private String branchPrefix;

    private ActivityType activityType;

    private Long conceptId;

    private String componentId;

    private Date commitDate;

    private Date fromDate;

    private Date toDate;

    private boolean intOnly;
    private boolean brief;
    private boolean summaryOnly;
    private boolean includeHigherPromotions;

    public String getOriginalBranch() {
        return originalBranch;
    }

    public void setOriginalBranch(String originalBranch) {
        this.originalBranch = originalBranch;
    }

    public String getOnBranch() {
        return onBranch;
    }

    public void setOnBranch(String onBranch) {
        this.onBranch = onBranch;
    }

    public String getSourceBranch() {
        return sourceBranch;
    }

    public void setSourceBranch(String sourceBranch) {
        this.sourceBranch = sourceBranch;
    }

    public String getBranchPrefix() {
        return branchPrefix;
    }

    public void setBranchPrefix(String branchPrefix) {
        this.branchPrefix = branchPrefix;
    }

    public ActivityType getActivityType() {
        return activityType;
    }

    public void setActivityType(ActivityType activityType) {
        this.activityType = activityType;
    }

    public Long getConceptId() {
        return conceptId;
    }

    public void setConceptId(Long conceptId) {
        this.conceptId = conceptId;
    }

    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(String componentId) {
        this.componentId = componentId;
    }

    public Date getCommitDate() {
        return commitDate;
    }

    public void setCommitDate(Date commitDate) {
        this.commitDate = commitDate;
    }

    public Date getFromDate() {
        return fromDate;
    }

    public void setFromDate(Date fromDate) {
        this.fromDate = fromDate;
    }

    public Date getToDate() {
        return toDate;
    }

    public void setToDate(Date toDate) {
        this.toDate = toDate;
    }

    public boolean isIntOnly() {
        return intOnly;
    }

    public void setIntOnly(boolean intOnly) {
        this.intOnly = intOnly;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ActivitySearchRequest.class.getSimpleName() + "[", "]")
                .add("originalBranch='" + originalBranch + "'")
                .add("onBranch='" + onBranch + "'")
                .add("sourceBranch='" + sourceBranch + "'")
                .add("branchPrefix='" + branchPrefix + "'")
                .add("activityType=" + activityType)
                .add("conceptId=" + conceptId)
                .add("componentId='" + componentId + "'")
                .add("commitDate=" + commitDate)
                .add("fromDate=" + fromDate)
                .add("toDate=" + toDate)
                .add("intOnly=" + intOnly)
                .add("brief=" + brief)
                .add("summaryOnly=" + summaryOnly)
                .toString();
    }

    public void setSummaryOnly(boolean summaryOnly) {
        this.summaryOnly = summaryOnly;
    }

    public boolean isSummaryOnly() {
        return summaryOnly;
    }

    public void setBrief(boolean brief) {
        this.brief = brief;
    }

    public boolean isBrief() {
        return brief;
    }

	public boolean isIncludeHigherPromotions() {
		return includeHigherPromotions;
	}

	public void setIncludeHigherPromotions(boolean includeHigherPromotions) {
		this.includeHigherPromotions = includeHigherPromotions;
	}
}
