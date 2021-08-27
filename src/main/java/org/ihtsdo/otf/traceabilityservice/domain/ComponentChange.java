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
	private String componentSubType;

	@Field(type = FieldType.Boolean)
	private boolean effectiveTimeNull;

	public ComponentChange() {
	}

	public ComponentChange(String componentId, ChangeType changeType, ComponentType componentType, String componentSubType, boolean effectiveTimeNull) {
		this.componentId = componentId;
		this.changeType = changeType;
		this.componentType = componentType;
		this.componentSubType = componentSubType;
		this.effectiveTimeNull = effectiveTimeNull;

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

	public String getComponentSubType() {
		return componentSubType;
	}

	public boolean isEffectiveTimeNull() {
		return effectiveTimeNull;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ComponentChange that = (ComponentChange) o;
		return effectiveTimeNull == that.effectiveTimeNull && componentId.equals(that.componentId) && changeType == that.changeType && componentType == that.componentType && Objects.equals(componentSubType, that.componentSubType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(componentId, changeType, componentType, componentSubType, effectiveTimeNull);
	}
}
