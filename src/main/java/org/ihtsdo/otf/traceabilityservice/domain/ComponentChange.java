package org.ihtsdo.otf.traceabilityservice.domain;

import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.util.Objects;

public class ComponentChange {

	@Field(type = FieldType.Keyword)
	private String componentId;

	@Field(type = FieldType.Keyword)
	private ChangeType changeType;

	@Field(type = FieldType.Keyword)
	private ComponentType componentType;

	@Field(type = FieldType.Keyword)
	private Long componentSubType;

	public ComponentChange() {
	}

	public ComponentChange(String componentId, ChangeType changeType, ComponentType componentType, Long componentSubType) {
		this.componentId = componentId;
		this.changeType = changeType;
		this.componentType = componentType;
		this.componentSubType = componentSubType;
	}

	public String getComponentId() {
		return componentId;
	}

	public ChangeType getChangeType() {
		return changeType;
	}

	public ComponentType getComponentType() {
		return componentType;
	}

	public Long getComponentSubType() {
		return componentSubType;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ComponentChange that = (ComponentChange) o;
		return componentId.equals(that.componentId) && changeType == that.changeType && componentType == that.componentType && Objects.equals(componentSubType, that.componentSubType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(componentId, changeType, componentType, componentSubType);
	}
}
