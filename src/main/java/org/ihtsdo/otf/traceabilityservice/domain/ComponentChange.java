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

	@Field(type = FieldType.Boolean)
	private Boolean superseded;

	public ComponentChange() {
	}

	public ComponentChange(String componentId, ChangeType changeType, ComponentType componentType, String componentSubType, boolean effectiveTimeNull) {
		this.componentId = componentId;
		this.changeType = changeType;
		this.componentType = componentType;
		this.componentSubType = componentSubType;
		this.effectiveTimeNull = effectiveTimeNull;
	}
	public ComponentChange(String componentId, ChangeType changeType, ComponentType componentType, String componentSubType, boolean effectiveTimeNull, Boolean superseded) {
		this(componentId, changeType, componentType, componentSubType, effectiveTimeNull);
		if (Boolean.TRUE == superseded) {
			// To make this field visible only when is true
			this.superseded = true;
		}
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

	public boolean isSuperseded() {
		return Boolean.TRUE == superseded;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ComponentChange that = (ComponentChange) o;
		return effectiveTimeNull == that.effectiveTimeNull && componentId.equals(that.componentId) && changeType == that.changeType
				&& componentType == that.componentType && Objects.equals(componentSubType, that.componentSubType) && Objects.equals(superseded, that.superseded);
	}

	@Override
	public int hashCode() {
		return Objects.hash(componentId, changeType, componentType, componentSubType, effectiveTimeNull, superseded);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder("ComponentChange{" +
				"componentId='" + componentId + '\'' +
				", changeType=" + changeType +
				", componentType=" + componentType +
				", componentSubType='" + componentSubType + '\'' +
				", effectiveTimeNull=" + effectiveTimeNull);
		if (isSuperseded()) {
			builder.append(", superseded=");
			builder.append(isSuperseded());
		}
		builder.append("}");
		return builder.toString();
	}
}
