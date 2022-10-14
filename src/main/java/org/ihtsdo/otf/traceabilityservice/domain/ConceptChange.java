package org.ihtsdo.otf.traceabilityservice.domain;

import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.util.HashSet;
import java.util.Set;

public class ConceptChange {

	@Field(type = FieldType.Keyword)
	private final String conceptId;

	@Field(type = FieldType.Object)
	private Set<ComponentChange> componentChanges;

	public ConceptChange(String conceptId) {
		this.conceptId = conceptId;
		componentChanges = new HashSet<>();
	}

	public String getConceptId() {
		return conceptId;
	}

	public ConceptChange addComponentChange(ComponentChange componentChange) {
		componentChanges.add(componentChange);
		return this;
	}

	public Set<ComponentChange> getComponentChanges() {
		return componentChanges;
	}

	public ConceptChange setComponentChanges(Set<ComponentChange> componentChanges) {
		this.componentChanges = componentChanges;
		return this;
	}
}
