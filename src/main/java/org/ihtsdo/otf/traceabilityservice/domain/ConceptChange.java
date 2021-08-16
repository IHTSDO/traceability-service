package org.ihtsdo.otf.traceabilityservice.domain;

import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.util.HashSet;
import java.util.Set;

public class ConceptChange {

	@Field(type = FieldType.Keyword)
	private final Long conceptId;

	private final Set<ComponentChange> componentChanges;

	public ConceptChange(long conceptId) {
		this.conceptId = conceptId;
		componentChanges = new HashSet<>();
	}

	public Long getConceptId() {
		return conceptId;
	}

	public void addComponentChange(ComponentChange componentChange) {
		componentChanges.add(componentChange);
	}

	public Set<ComponentChange> getComponentChanges() {
		return componentChanges;
	}
}
