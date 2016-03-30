package org.ihtsdo.otf.traceabilityservice.domain;

import javax.persistence.*;

@Entity
public class Branch {

	@Id
	@Column(name = "id")
	@GeneratedValue(strategy=GenerationType.AUTO)
	private Long id;

	@Column(nullable = false)
	private String branchPath;

	public Branch() {
	}

	public Branch(String branchPath) {
		this.branchPath = branchPath;
	}

	public Long getId() {
		return id;
	}

	public String getBranchPath() {
		return branchPath;
	}

	@Override
	public String toString() {
		return "Branch{" +
				"id=" + id +
				", branchPath='" + branchPath + '\'' +
				'}';
	}
}
