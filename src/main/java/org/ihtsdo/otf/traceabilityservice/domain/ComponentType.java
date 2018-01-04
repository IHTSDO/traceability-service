package org.ihtsdo.otf.traceabilityservice.domain;

public enum ComponentType {
	// NB - New enums must go at the end of the list because of the JPA integer mapping
	CONCEPT, DESCRIPTION, RELATIONSHIP, OWLAXIOM

}
