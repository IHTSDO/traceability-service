package org.ihtsdo.otf.traceabilityservice.service;

import org.ihtsdo.otf.traceabilityservice.domain.ComponentType;

public class ComponentIdUtil {

	private ComponentIdUtil() {
	}

	public static ComponentType getComponentType(String id) {
		ComponentType componentType = null;
		if (id.contains("-")) {
			componentType = ComponentType.REFERENCE_SET_MEMBER;
		} else {
			int partitionId = Integer.parseInt(id.substring(id.length() - 2, id.length() - 1));
			if (partitionId == 0) {
				componentType = ComponentType.CONCEPT;
			} else if (partitionId == 1) {
				componentType = ComponentType.DESCRIPTION;
			} else if (partitionId == 2) {
				componentType = ComponentType.RELATIONSHIP;
			}
		}
		return componentType;
	}
}
