package org.ihtsdo.otf.traceabilityservice.domain;

import java.util.HashSet;
import java.util.Set;
import javax.persistence.*;

@Entity
public class ConceptChange {

	@Id
	@Column(name = "id")
	@GeneratedValue(strategy= GenerationType.AUTO)
	private Long id;

	@ManyToOne
	@JoinColumn(name="activity_id", nullable=false)
	private Activity activity;

	private Long conceptId;

	@OneToMany(cascade = CascadeType.ALL, mappedBy = "conceptChange", fetch = FetchType.EAGER)
	private Set<ComponentChange> componentChanges;

	public ConceptChange() {
	}

	public ConceptChange(Long conceptId) {
		this.conceptId = conceptId;
		this.componentChanges = new HashSet<>();
	}

	public void addComponentChange(ComponentChange componentChange) {
		componentChanges.add(componentChange);
		componentChange.setConceptChange(this);
	}

	public void setActivity(Activity activity) {
		this.activity = activity;
	}

	public Long getId() {
		return id;
	}

	public Activity getActivity() {
		return activity;
	}

	public Long getConceptId() {
		return conceptId;
	}

	public Set<ComponentChange> getComponentChanges() {
		return componentChanges;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ConceptChange that = (ConceptChange) o;

		if (conceptId != null ? !conceptId.equals(that.conceptId) : that.conceptId != null) return false;
		return componentChanges != null ? componentChanges.equals(that.componentChanges) : that.componentChanges == null;

	}

	@Override
	public int hashCode() {
		int result = conceptId != null ? conceptId.hashCode() : 0;
		result = 31 * result + (componentChanges != null ? componentChanges.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "ConceptChange{" +
				"id=" + id +
				", conceptId=" + conceptId +
				", componentChanges=" + componentChanges +
				'}';
	}
}
