package org.ihtsdo.otf.traceabilityservice.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.persistence.*;

@Entity
public class ComponentChange {

	@Id
	@Column(name = "id")
	@GeneratedValue(strategy=GenerationType.AUTO)
	private Long id;

	@ManyToOne
	@JoinColumn(name = "concept_change_id")
	@JsonIgnore
	private ConceptChange conceptChange;

	private String componentId;

	@Enumerated
	private ComponentType componentType;

	@Enumerated
	private ComponentChangeType changeType;

	public ComponentChange() {
	}

	public ComponentChange(ComponentType componentType, String componentId, ComponentChangeType changeType) {
		this.componentType = componentType;
		this.componentId = componentId;
		this.changeType = changeType;
	}

	public void setConceptChange(ConceptChange conceptChange) {
		this.conceptChange = conceptChange;
	}

	public ComponentType getComponentType() {
		return componentType;
	}

	public String getComponentId() {
		return componentId;
	}

	public ComponentChangeType getChangeType() {
		return changeType;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ComponentChange that = (ComponentChange) o;

		if (componentType != that.componentType) return false;
		if (componentId != null ? !componentId.equals(that.componentId) : that.componentId != null) return false;
		return changeType == that.changeType;

	}

	@Override
	public int hashCode() {
		int result = componentType != null ? componentType.hashCode() : 0;
		result = 31 * result + (componentId != null ? componentId.hashCode() : 0);
		result = 31 * result + (changeType != null ? changeType.hashCode() : 0);
		return result;
	}
}
