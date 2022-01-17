package org.ihtsdo.otf.traceabilityservice.domain;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ChangeSummaryReport {

	private final Map<ComponentType, Set<String>> componentChanges;
	private final List<Activity> changesNotAtTaskLevel;
	private Map<String, String> componentToConceptIdMap;

	public ChangeSummaryReport(Map<ComponentType, Set<String>> componentChanges, List<Activity> changesNotAtTaskLevel) {
		this.componentChanges = componentChanges;
		this.changesNotAtTaskLevel = changesNotAtTaskLevel;
	}

	public Map<ComponentType, Set<String>> getComponentChanges() {
		return componentChanges;
	}

	public List<Activity> getChangesNotAtTaskLevel() {
		return changesNotAtTaskLevel;
	}

	public void setComponentToConceptIdMap(Map<String, String> componentToConceptIdMap) {
		this.componentToConceptIdMap = componentToConceptIdMap;
	}

	public Map<String, String> getComponentToConceptIdMap() {
		return componentToConceptIdMap;
	}

	@Override
	public String toString() {
		return "ChangeSummaryReport{" +
				"componentChanges=" + componentChanges +
				", changesNotAtTaskLevel=" + changesNotAtTaskLevel +
				'}';
	}

}
