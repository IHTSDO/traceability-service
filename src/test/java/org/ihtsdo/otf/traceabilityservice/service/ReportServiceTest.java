package org.ihtsdo.otf.traceabilityservice.service;

import com.google.common.collect.Lists;
import org.ihtsdo.otf.traceabilityservice.AbstractTest;
import org.ihtsdo.otf.traceabilityservice.domain.*;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

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
		assertTrue(BranchUtil.isCodeSystemBranch("MAIN"));
		assertFalse(BranchUtil.isCodeSystemBranch("MAIN/PROJECT"));
		assertFalse(BranchUtil.isCodeSystemBranch("MAIN/PROJECT/PROJECT-123"));
		assertFalse(BranchUtil.isCodeSystemBranch("MAIN/2021-09-30"));
		assertTrue(BranchUtil.isCodeSystemBranch("MAIN/SNOMEDCT-BE"));
		assertFalse(BranchUtil.isCodeSystemBranch("MAIN/SNOMEDCT-BE/BE"));
		assertFalse(BranchUtil.isCodeSystemBranch("MAIN/SNOMEDCT-BE/BE/BE-123"));
		assertTrue(BranchUtil.isCodeSystemBranch("MAIN/SNOMEDCT-ES"));
		assertTrue(BranchUtil.isCodeSystemBranch("MAIN/SNOMEDCT-ES/SNOMEDCT-AR"));
		assertFalse(BranchUtil.isCodeSystemBranch("MAIN/SNOMEDCT-ES/SNOMEDCT-AR/AR"));
		assertFalse(BranchUtil.isCodeSystemBranch("MAIN/SNOMEDCT-ES/SNOMEDCT-AR/AR/AR-123"));
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
		Map<String, String> componentToConceptIdMap = changeSummaryReport.getComponentToConceptIdMap();
		assertNotNull(componentToConceptIdMap);
		changeSummaryReport.getComponentChanges().values().stream().flatMap(Collection::stream).forEach(componentId -> {
			assertEquals("100", componentToConceptIdMap.get(componentId));
		});
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
		assertEquals("{}", toString(changeSummaryReport.getComponentChanges()));
		assertEquals(0, changeSummaryReport.getChangesNotAtTaskLevel().size());

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
		assertEquals(0, changeSummaryReport.getChangesNotAtTaskLevel().size());
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
		assertEquals("{}", toString(changeSummaryReport.getComponentChanges()));
		assertEquals(0, changeSummaryReport.getChangesNotAtTaskLevel().size());
	}

	@Test
	void testCreateConceptThenVersion() {
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN", null, ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("100", ChangeType.CREATE, ComponentType.CONCEPT, "", true))
								.addComponentChange(new ComponentChange("110", ChangeType.CREATE, ComponentType.DESCRIPTION, "", true))
								.addComponentChange(new ComponentChange("a1", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, "", true))
								.addComponentChange(new ComponentChange("a2", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, "", true))
								.addComponentChange(new ComponentChange("120", ChangeType.CREATE, ComponentType.RELATIONSHIP, "", true))
						)
		));

		final ChangeSummaryReport reportBeforeVersioning = reportService.createChangeSummaryReport("MAIN");
		assertEquals("{CONCEPT=[100], DESCRIPTION=[110], RELATIONSHIP=[120], REFERENCE_SET_MEMBER=[a1, a2]}", toString(reportBeforeVersioning.getComponentChanges()));
		assertEquals(1, reportBeforeVersioning.getChangesNotAtTaskLevel().size());

		activityRepository.save(activity("MAIN", null, ActivityType.CREATE_CODE_SYSTEM_VERSION));

		final ChangeSummaryReport reportAfterVersioning = reportService.createChangeSummaryReport("MAIN");
		assertEquals("{}", toString(reportAfterVersioning.getComponentChanges()));
		assertEquals(0, reportAfterVersioning.getChangesNotAtTaskLevel().size());
	}

	@Test
	void testParentActivityBeforeAndAfterVersioning() {
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN", null, ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("100", ChangeType.CREATE, ComponentType.CONCEPT, "", true))
								.addComponentChange(new ComponentChange("110", ChangeType.CREATE, ComponentType.DESCRIPTION, "", true))
								.addComponentChange(new ComponentChange("a1", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, "", true))
								.addComponentChange(new ComponentChange("a2", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, "", true))
								.addComponentChange(new ComponentChange("120", ChangeType.CREATE, ComponentType.RELATIONSHIP, "", true))
						)
		));

		activityRepository.save(activity("MAIN/A", "MAIN", ActivityType.REBASE));

		final ChangeSummaryReport projectReportBeforeVersioning = reportService.createChangeSummaryReport("MAIN/A");
		assertEquals("{CONCEPT=[100], DESCRIPTION=[110], RELATIONSHIP=[120], REFERENCE_SET_MEMBER=[a1, a2]}",
				toString(projectReportBeforeVersioning.getComponentChanges()),
				"Unversioned content inherited by project.");
		assertEquals(1, projectReportBeforeVersioning.getChangesNotAtTaskLevel().size());

		activityRepository.save(activity("MAIN", null, ActivityType.CREATE_CODE_SYSTEM_VERSION));

		final ChangeSummaryReport projectReportAfterVersioningBeforeRebase = reportService.createChangeSummaryReport("MAIN/A");
		assertEquals("{CONCEPT=[100], DESCRIPTION=[110], RELATIONSHIP=[120], REFERENCE_SET_MEMBER=[a1, a2]}",
				toString(projectReportAfterVersioningBeforeRebase.getComponentChanges()),
				"Content on parent versioned but update not yet rebased into project.");
		assertEquals(1, projectReportAfterVersioningBeforeRebase.getChangesNotAtTaskLevel().size());

		activityRepository.save(activity("MAIN/A", "MAIN", ActivityType.REBASE));

		final ChangeSummaryReport projectReportAfterVersioningAfterRebase = reportService.createChangeSummaryReport("MAIN/A");
		assertEquals("{}", toString(projectReportAfterVersioningAfterRebase.getComponentChanges()),
				"Versioned content rebased into project.");
		assertEquals(0, projectReportAfterVersioningAfterRebase.getChangesNotAtTaskLevel().size());
	}

	@Test
	void testParentActivityBeforeAndAfterVersioningWithLocalChange() {
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN", null, ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("100", ChangeType.CREATE, ComponentType.CONCEPT, "", true))
								.addComponentChange(new ComponentChange("110", ChangeType.CREATE, ComponentType.DESCRIPTION, "", true))
								.addComponentChange(new ComponentChange("a1", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, "", true))
								.addComponentChange(new ComponentChange("a2", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, "", true))
								.addComponentChange(new ComponentChange("120", ChangeType.CREATE, ComponentType.RELATIONSHIP, "", true))
						),
				activity("MAIN/A", null, ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("200")
								.addComponentChange(new ComponentChange("200", ChangeType.CREATE, ComponentType.CONCEPT, "", true))
								.addComponentChange(new ComponentChange("210", ChangeType.CREATE, ComponentType.DESCRIPTION, "", true))
								.addComponentChange(new ComponentChange("b1", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, "", true))
								.addComponentChange(new ComponentChange("b2", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, "", true))
								.addComponentChange(new ComponentChange("220", ChangeType.CREATE, ComponentType.RELATIONSHIP, "", true))
						)
		));

		activityRepository.save(activity("MAIN/A", "MAIN", ActivityType.REBASE));

		final ChangeSummaryReport projectReportBeforeVersioning = reportService.createChangeSummaryReport("MAIN/A");
		assertEquals("{CONCEPT=[100, 200], DESCRIPTION=[110, 210], RELATIONSHIP=[120, 220], REFERENCE_SET_MEMBER=[a1, a2, b1, b2]}",
				toString(projectReportBeforeVersioning.getComponentChanges()),
				"Unversioned content inherited by project.");
		assertEquals(2, projectReportBeforeVersioning.getChangesNotAtTaskLevel().size());

		activityRepository.save(activity("MAIN", null, ActivityType.CREATE_CODE_SYSTEM_VERSION));

		final ChangeSummaryReport projectReportAfterVersioningBeforeRebase = reportService.createChangeSummaryReport("MAIN/A");
		assertEquals("{CONCEPT=[100, 200], DESCRIPTION=[110, 210], RELATIONSHIP=[120, 220], REFERENCE_SET_MEMBER=[a1, a2, b1, b2]}",
				toString(projectReportAfterVersioningBeforeRebase.getComponentChanges()),
				"Content on parent versioned but update not yet rebased into project.");
		assertEquals(2, projectReportAfterVersioningBeforeRebase.getChangesNotAtTaskLevel().size());

		activityRepository.save(activity("MAIN/A", "MAIN", ActivityType.REBASE));

		final ChangeSummaryReport projectReportAfterVersioningAfterRebase = reportService.createChangeSummaryReport("MAIN/A");
		assertEquals("{CONCEPT=[200], DESCRIPTION=[210], RELATIONSHIP=[220], REFERENCE_SET_MEMBER=[b1, b2]}",
				toString(projectReportAfterVersioningAfterRebase.getComponentChanges()),
				"Versioned content rebased into project. Unversioned content at project level still visible.");
		assertEquals(1, projectReportAfterVersioningAfterRebase.getChangesNotAtTaskLevel().size());
	}

	/*

	// TODO Test manually.

		Hard case .
		concept versioned on MAIN
		concept changed on MAIN/A
		concept changed and changed back on MAIN/B
		A promoted
		B rebased .. will record correct date flag
		B promoted
		.. what's in the log
		last commit will have correct flag because it's the rebase commit that happened afterwards
		 */

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