package org.ihtsdo.otf.traceabilityservice.domain;

import java.util.Map;
import java.util.Set;

public record DiffReport(Map<ComponentType, Set<String>> missingFromDelta,
						 Map<ComponentType, Set<String>> missingFromStore) {


}
