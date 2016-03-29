package org.ihtsdo.otf.traceabilityservice.domain;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import javax.persistence.*;

@Entity
public class Activity {

	@Id
	@GeneratedValue(strategy= GenerationType.AUTO)
	private Long id;

	@ManyToOne
	private Branch branch;

	private String userId;
	private String commitComment;
	private Date commitDate;
	@Enumerated
	private ActivityType activityType;

	@OneToMany(cascade = CascadeType.ALL, mappedBy = "activity", fetch = FetchType.EAGER)
	private Set<ConceptChange> conceptChanges;

	public Activity() {
	}

	public Activity(String userId, String commitComment, Branch branch, Date commitDate, ActivityType activityType) {
		this.userId = userId;
		this.commitComment = commitComment;
		this.branch = branch;
		this.commitDate = commitDate;
		this.activityType = activityType;
		conceptChanges = new HashSet<>();
	}

	public void addConceptChange(ConceptChange conceptChange) {
		conceptChanges.add(conceptChange);
		conceptChange.setActivity(this);
	}

	public Long getId() {
		return id;
	}

	public String getUserId() {
		return userId;
	}

	public String getCommitComment() {
		return commitComment;
	}

	public Branch getBranch() {
		return branch;
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
}
