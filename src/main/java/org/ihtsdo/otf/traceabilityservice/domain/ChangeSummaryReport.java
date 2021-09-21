package org.ihtsdo.otf.traceabilityservice.domain;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ChangeSummaryReport {

	private final Map<ComponentType, Set<String>> componentChanges;
	private final List<Activity> changesNotAtTaskLevel;

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

	@Override
	public String toString() {
		return "ChangeSummaryReport{" +
				"componentChanges=" + componentChanges +
				", changesNotAtTaskLevel=" + changesNotAtTaskLevel +
				'}';
	}
}
