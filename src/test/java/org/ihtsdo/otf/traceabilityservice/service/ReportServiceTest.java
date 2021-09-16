package org.ihtsdo.otf.traceabilityservice.service;

import com.google.common.collect.Lists;
import org.ihtsdo.otf.traceabilityservice.AbstractTest;
import org.ihtsdo.otf.traceabilityservice.domain.*;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ReportServiceTest extends AbstractTest {

	@Autowired
	private ReportService reportService;

	@Autowired
	private ActivityRepository activityRepository;

	@AfterEach
	void after() {
		activityRepository.deleteAll();
	}

	@Test
	void testCreateAncestorDeque() {
		final Deque<String> ancestors = reportService.createAncestorDeque("MAIN/PROJECTA/PROJECTA-1");
		assertEquals(2, ancestors.size());
		assertEquals("MAIN/PROJECTA", ancestors.pop());
		assertEquals("MAIN", ancestors.pop());
	}

	@Test
	void testIsCodeSystemBranch() {
		assertTrue(reportService.isCodeSystemBranch("MAIN"));
		assertFalse(reportService.isCodeSystemBranch("MAIN/PROJECT"));
		assertFalse(reportService.isCodeSystemBranch("MAIN/PROJECT/PROJECT-123"));
		assertFalse(reportService.isCodeSystemBranch("MAIN/2021-09-30"));
		assertTrue(reportService.isCodeSystemBranch("MAIN/SNOMEDCT-BE"));
		assertFalse(reportService.isCodeSystemBranch("MAIN/SNOMEDCT-BE/BE"));
		assertFalse(reportService.isCodeSystemBranch("MAIN/SNOMEDCT-BE/BE/BE-123"));
		assertTrue(reportService.isCodeSystemBranch("MAIN/SNOMEDCT-ES"));
		assertTrue(reportService.isCodeSystemBranch("MAIN/SNOMEDCT-ES/SNOMEDCT-AR"));
		assertFalse(reportService.isCodeSystemBranch("MAIN/SNOMEDCT-ES/SNOMEDCT-AR/AR"));
		assertFalse(reportService.isCodeSystemBranch("MAIN/SNOMEDCT-ES/SNOMEDCT-AR/AR/AR-123"));
	}

	@Test
	void testCreateConcept() {
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A/A-1", "", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("100", ChangeType.CREATE, ComponentType.CONCEPT, "", true))
								.addComponentChange(new ComponentChange("110", ChangeType.CREATE, ComponentType.DESCRIPTION, "", true))
								.addComponentChange(new ComponentChange("a1", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, "", true))
								.addComponentChange(new ComponentChange("a2", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, "", true))
								.addComponentChange(new ComponentChange("120", ChangeType.CREATE, ComponentType.RELATIONSHIP, "", true))
						)
		));

		final ChangeSummaryReport changeSummaryReport = reportService.createChangeSummaryReport("MAIN/A/A-1");

		assertEquals("{CONCEPT=[100], DESCRIPTION=[110], RELATIONSHIP=[120], REFERENCE_SET_MEMBER=[a1, a2]}",
				toString(changeSummaryReport.getComponentChanges()));
	}

	@Test
	void testUpdateRevertReleasedComponent() {
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A/A-1", "", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("110", ChangeType.UPDATE, ComponentType.DESCRIPTION, "", true))
						),
				activity("MAIN/A/A-1", "", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("110", ChangeType.UPDATE, ComponentType.DESCRIPTION, "", false))
						)
		));

		final ChangeSummaryReport changeSummaryReport = reportService.createChangeSummaryReport("MAIN/A/A-1");

		assertEquals("{}",
				toString(changeSummaryReport.getComponentChanges()));
	}

	@Test
	void testCreateConceptDeleteDescription() {
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A/A-1", "", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("100", ChangeType.CREATE, ComponentType.CONCEPT, "", true))
								.addComponentChange(new ComponentChange("110", ChangeType.CREATE, ComponentType.DESCRIPTION, "", true))
								.addComponentChange(new ComponentChange("a1", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, "", true))
								.addComponentChange(new ComponentChange("a2", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, "", true))
								.addComponentChange(new ComponentChange("120", ChangeType.CREATE, ComponentType.RELATIONSHIP, "", true))
						),
				activity("MAIN/A/A-1", "", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("110", ChangeType.DELETE, ComponentType.DESCRIPTION, "", true))
								.addComponentChange(new ComponentChange("a1", ChangeType.DELETE, ComponentType.REFERENCE_SET_MEMBER, "", true))
								.addComponentChange(new ComponentChange("a2", ChangeType.DELETE, ComponentType.REFERENCE_SET_MEMBER, "", true))
						)
		));

		final ChangeSummaryReport changeSummaryReport = reportService.createChangeSummaryReport("MAIN/A/A-1");

		assertEquals("{CONCEPT=[100], RELATIONSHIP=[120]}",
				toString(changeSummaryReport.getComponentChanges()));
	}

	@Test
	void testUpdateDescriptionRebaseDeletion() {
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A/A-1", "", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("110", ChangeType.UPDATE, ComponentType.DESCRIPTION, "", true))
						),
				activity("MAIN/A/A-2", "", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("110", ChangeType.DELETE, ComponentType.DESCRIPTION, "", true))
						),
				activity("MAIN/A", "MAIN/A/A-2", ActivityType.PROMOTION),
				activity("MAIN/A/A-1", "MAIN/A", ActivityType.REBASE)
						// The manual or automatic concept merge function will log the concept changes on the target branch during the rebase
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("110", ChangeType.DELETE, ComponentType.DESCRIPTION, "", true))
						)
		));

		final ChangeSummaryReport changeSummaryReport = reportService.createChangeSummaryReport("MAIN/A/A-1");

		assertEquals("{}",
				toString(changeSummaryReport.getComponentChanges()));
	}

	private int testTime = 0;

	private Activity activity(String branchPath, String sourceBranch, ActivityType contentChange) {
		return new Activity("test", branchPath, sourceBranch, new Date(new Date().getTime() + testTime++), contentChange);
	}

	private String toString(Map<ComponentType, Set<String>> componentChanges) {
		final EnumMap<ComponentType, Object> sortedMap = new EnumMap<>(ComponentType.class);
		for (Map.Entry<ComponentType, Set<String>> entry : componentChanges.entrySet()) {
			sortedMap.put(entry.getKey(), new TreeSet<>(entry.getValue()));
		}
		return sortedMap.toString();
	}

}