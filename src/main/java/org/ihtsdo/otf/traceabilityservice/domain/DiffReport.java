package org.ihtsdo.otf.traceabilityservice.domain;

import java.util.Map;
import java.util.Set;

public class DiffReport {

	private final Map<ComponentType, Set<String>> missingFromDelta;
	private final Map<ComponentType, Set<String>> missingFromStore;

	public DiffReport(Map<ComponentType, Set<String>> missingFromDelta, Map<ComponentType, Set<String>> missingFromStore) {
		this.missingFromDelta = missingFromDelta;
		this.missingFromStore = missingFromStore;
	}

	public Map<ComponentType, Set<String>> getMissingFromDelta() {
		return missingFromDelta;
	}

	public Map<ComponentType, Set<String>> getMissingFromStore() {
		return missingFromStore;
	}
}
