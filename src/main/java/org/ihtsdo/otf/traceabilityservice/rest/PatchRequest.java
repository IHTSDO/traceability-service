package org.ihtsdo.otf.traceabilityservice.rest;

import java.util.HashSet;
import java.util.Set;

public class PatchRequest {

	private String branch;
	private Set<String> componentsWithEffectiveTime;
	private Set<String> componentsWithoutEffectiveTime;

	public PatchRequest() {
		componentsWithEffectiveTime = new HashSet<>();
		componentsWithoutEffectiveTime = new HashSet<>();
	}

	public String getBranch() {
		return branch;
	}

	public Set<String> getComponentsWithEffectiveTime() {
		return componentsWithEffectiveTime;
	}

	public Set<String> getComponentsWithoutEffectiveTime() {
		return componentsWithoutEffectiveTime;
	}
}
