package org.ihtsdo.otf.traceabilityservice.service;

import com.google.common.collect.Sets;
import org.ihtsdo.otf.traceabilityservice.domain.ChangeSummaryReport;
import org.ihtsdo.otf.traceabilityservice.domain.ComponentType;
import org.ihtsdo.otf.traceabilityservice.domain.DiffReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Service
public class ArchiveDiffService {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	public DiffReport diff(ChangeSummaryReport reportFromStore, InputStream rf2DeltaArchive) throws IOException {
		ChangeSummaryReport reportFromArchive = readArchive(rf2DeltaArchive);

		final Map<ComponentType, Set<String>> storeChanges = reportFromStore.getComponentChanges();
		final Map<ComponentType, Set<String>> deltaChanges = reportFromArchive.getComponentChanges();

		final Map<ComponentType, Set<String>> missingFromDelta = new EnumMap<>(ComponentType.class);
		final Map<ComponentType, Set<String>> missingFromStore = new EnumMap<>(ComponentType.class);

		putDiff(missingFromDelta, ComponentType.CONCEPT, storeChanges, deltaChanges);
		putDiff(missingFromDelta, ComponentType.DESCRIPTION, storeChanges, deltaChanges);
		putDiff(missingFromDelta, ComponentType.RELATIONSHIP, storeChanges, deltaChanges);
		putDiff(missingFromDelta, ComponentType.REFERENCE_SET_MEMBER, storeChanges, deltaChanges);

		putDiff(missingFromStore, ComponentType.CONCEPT, deltaChanges, storeChanges);
		putDiff(missingFromStore, ComponentType.DESCRIPTION, deltaChanges, storeChanges);
		putDiff(missingFromStore, ComponentType.RELATIONSHIP, deltaChanges, storeChanges);
		putDiff(missingFromStore, ComponentType.REFERENCE_SET_MEMBER, deltaChanges, storeChanges);

		return new DiffReport(missingFromDelta, missingFromStore);
	}

	private void putDiff(Map<ComponentType, Set<String>> results, ComponentType type, Map<ComponentType, Set<String>> leftSide, Map<ComponentType, Set<String>> rightSide) {
		results.put(type, Sets.difference(leftSide.getOrDefault(type, Collections.emptySet()), rightSide.getOrDefault(type, Collections.emptySet())));
	}

	private ChangeSummaryReport readArchive(InputStream rf2DeltaArchive) throws IOException {
		Map<ComponentType, Set<String>> componentChangesMap = new EnumMap<>(ComponentType.class);

		try (ZipInputStream zipStream = new ZipInputStream(rf2DeltaArchive, StandardCharsets.UTF_8)) {
			ZipEntry entry;
			while ((entry = zipStream.getNextEntry()) != null) {
				BufferedReader reader = new BufferedReader(new InputStreamReader(zipStream));
				final String header = reader.readLine();
				logger.info("Reading file {} with header {}", entry.getName(), header);
				String line;
				String[] columns;
				String id;
				while ((line = reader.readLine()) != null) {
					columns = line.split("\\t");
					id = columns[0];
					ComponentType componentType = ComponentIdUtil.getComponentType(id);
					if (componentType != null) {
						componentChangesMap.computeIfAbsent(componentType, type -> new HashSet<>()).add(id);
					}
				}
			}
		}
		return new ChangeSummaryReport(componentChangesMap, null);
	}

}
