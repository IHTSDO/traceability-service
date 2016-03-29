package org.ihtsdo.otf.traceabilityservice.domain;

public enum ComponentChangeType {
	// NB - New enums must go at the end of the list because of the JPA integer mapping
	CREATE, UPDATE, INACTIVATE, DELETE

}
