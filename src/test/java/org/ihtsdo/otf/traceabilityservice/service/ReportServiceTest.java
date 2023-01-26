package org.ihtsdo.otf.traceabilityservice.service;

import com.google.common.collect.Lists;
import org.ihtsdo.otf.traceabilityservice.AbstractTest;
import org.ihtsdo.otf.traceabilityservice.domain.*;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHitsIterator;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;

import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.junit.jupiter.api.Assertions.*;

class ReportServiceTest extends AbstractTest {

	@Autowired
	private ReportService reportService;

	@Autowired
	private ActivityRepository activityRepository;

	@Autowired
	private ElasticsearchOperations elasticsearchOperations;

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
		assertTrue(BranchUtils.isCodeSystemBranch("MAIN"));
		assertFalse(BranchUtils.isCodeSystemBranch("MAIN/PROJECT"));
		assertFalse(BranchUtils.isCodeSystemBranch("MAIN/PROJECT/PROJECT-123"));
		assertFalse(BranchUtils.isCodeSystemBranch("MAIN/2021-09-30"));
		assertTrue(BranchUtils.isCodeSystemBranch("MAIN/SNOMEDCT-BE"));
		assertFalse(BranchUtils.isCodeSystemBranch("MAIN/SNOMEDCT-BE/BE"));
		assertFalse(BranchUtils.isCodeSystemBranch("MAIN/SNOMEDCT-BE/BE/BE-123"));
		assertTrue(BranchUtils.isCodeSystemBranch("MAIN/SNOMEDCT-ES"));
		assertTrue(BranchUtils.isCodeSystemBranch("MAIN/SNOMEDCT-ES/SNOMEDCT-AR"));
		assertFalse(BranchUtils.isCodeSystemBranch("MAIN/SNOMEDCT-ES/SNOMEDCT-AR/AR"));
		assertFalse(BranchUtils.isCodeSystemBranch("MAIN/SNOMEDCT-ES/SNOMEDCT-AR/AR/AR-123"));
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
		changeSummaryReport.getComponentChanges().values().stream().flatMap(Collection::stream).forEach(componentId -> assertEquals("100", componentToConceptIdMap.get(componentId)));
	}

	@Test
	void testSummaryReportWithTimeCutOff() throws Exception {
		final long firstBaseHeadTime = System.currentTimeMillis();
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A", "", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("300")
								.addComponentChange(new ComponentChange("300", ChangeType.CREATE, ComponentType.CONCEPT, "", true))
								.addComponentChange(new ComponentChange("310", ChangeType.CREATE, ComponentType.DESCRIPTION, "", true))
								.addComponentChange(new ComponentChange("c1", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, "", true))
								.addComponentChange(new ComponentChange("c2", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, "", true))
								.addComponentChange(new ComponentChange("320", ChangeType.CREATE, ComponentType.RELATIONSHIP, "", true))
						)
		));

		final long secondBaseHeadTime = System.currentTimeMillis();

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

		final long firstTimeCutOff = System.currentTimeMillis();

		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A/A-1", "", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("200")
								.addComponentChange(new ComponentChange("200", ChangeType.CREATE, ComponentType.CONCEPT, "", true))
								.addComponentChange(new ComponentChange("210", ChangeType.CREATE, ComponentType.DESCRIPTION, "", true))
								.addComponentChange(new ComponentChange("b1", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, "", true))
								.addComponentChange(new ComponentChange("b2", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, "", true))
								.addComponentChange(new ComponentChange("220", ChangeType.CREATE, ComponentType.RELATIONSHIP, "", true))
						)
		));

		final long secondTimeCutOff = System.currentTimeMillis();

		ChangeSummaryReport changeSummaryReport = reportService.createChangeSummaryReport("MAIN/A/A-1", firstBaseHeadTime-1, firstTimeCutOff, false, false, true);
		assertTrue(changeSummaryReport.getComponentChanges().isEmpty());

		changeSummaryReport = reportService.createChangeSummaryReport("MAIN/A/A-1", firstBaseHeadTime-1, firstTimeCutOff);
		assertEquals("{CONCEPT=[100], DESCRIPTION=[110], RELATIONSHIP=[120], REFERENCE_SET_MEMBER=[a1, a2]}",
				toString(changeSummaryReport.getComponentChanges()));

		changeSummaryReport = reportService.createChangeSummaryReport("MAIN/A/A-1", secondBaseHeadTime-1, firstTimeCutOff);
		assertEquals("{CONCEPT=[100, 300], DESCRIPTION=[110, 310], RELATIONSHIP=[120, 320], REFERENCE_SET_MEMBER=[a1, a2, c1, c2]}",
				toString(changeSummaryReport.getComponentChanges()));

		changeSummaryReport = reportService.createChangeSummaryReport("MAIN/A/A-1", secondBaseHeadTime-1, secondTimeCutOff);
		assertEquals("{CONCEPT=[100, 200, 300], DESCRIPTION=[110, 210, 310], RELATIONSHIP=[120, 220, 320], REFERENCE_SET_MEMBER=[a1, a2, b1, b2, c1, c2]}",
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

	@Test
	void testPromotionSummaryReport() {
		// Create a concept on a task
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A/task1", null, ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("100", ChangeType.CREATE, ComponentType.CONCEPT, "", true)))
		));
		// Promote to project
		promoteActivities("MAIN/A/task1", "MAIN/A");
		activityRepository.save(activity("MAIN/A", "MAIN/A/task1", ActivityType.PROMOTION));

		// Versioning on MAIN
		activityRepository.save(activity("MAIN", null, ActivityType.CREATE_CODE_SYSTEM_VERSION));

		// Rebase project A with MAIN
		activityRepository.save(activity("MAIN/A", "MAIN", ActivityType.REBASE));

		// Run summary report on project
		final ChangeSummaryReport projectReportAfterVersioningAfterRebase = reportService.createChangeSummaryReport("MAIN/A", null, null, false, true, false);
		// Changes should be found
		assertEquals("{CONCEPT=[100]}", toString(projectReportAfterVersioningAfterRebase.getComponentChanges()));

		// Promote project to MAIN
		promoteActivities("MAIN/A", "MAIN");
		activityRepository.save(activity("MAIN/A", "MAIN", ActivityType.PROMOTION));

		// Run summary report on MAIN
		final ChangeSummaryReport reportOnMain = reportService.createChangeSummaryReport("MAIN", null, null, false, true, false);
		// Changes should be found
		assertEquals("{CONCEPT=[100]}", toString(reportOnMain.getComponentChanges()));

		// Create another task and promote to project A
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A/task2", null, ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("200")
								.addComponentChange(new ComponentChange("200", ChangeType.CREATE, ComponentType.CONCEPT, "", true)))
		));

		// Promote to project
		promoteActivities("MAIN/A/task2", "MAIN/A");
		activityRepository.save(activity("MAIN/A", "MAIN/A/task2", ActivityType.PROMOTION));
		activityRepository.save(activity("MAIN/A", "MAIN", ActivityType.REBASE));

		final ChangeSummaryReport promotionReportOnProject = reportService.createChangeSummaryReport("MAIN/A", null, null, false, true, false);
		// Promotion changes should be found in project
		assertEquals("{CONCEPT=[200]}", toString(promotionReportOnProject.getComponentChanges()));

		final ChangeSummaryReport rebaseReportOnProject = reportService.createChangeSummaryReport("MAIN/A", null, null, false, false, true);
		// Previous promoted changes to MAIN should be found in the rebase report
		assertEquals("{CONCEPT=[100]}", toString(rebaseReportOnProject.getComponentChanges()));
	}


	@Test
	void testRebaseSummaryReport() {
		// Create a concept on a task1
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A/task1", null, ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("100", ChangeType.CREATE, ComponentType.CONCEPT, "", true)))
		));

		// Promote task 1 to project
		promoteActivities("MAIN/A/task1", "MAIN/A");
		activityRepository.save(activity("MAIN/A", "MAIN/A/task1", ActivityType.PROMOTION));

		// Promote project A to MAIN
		promoteActivities("MAIN/A", "MAIN");
		activityRepository.save(activity("MAIN", "MAIN/A", ActivityType.PROMOTION));

		// Rebase project
		activityRepository.save(activity("MAIN/A", "MAIN", ActivityType.REBASE));

		// Create task2
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A/task2", null, ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("200")
								.addComponentChange(new ComponentChange("200", ChangeType.CREATE, ComponentType.CONCEPT, "", true)))
		));

		// Promote task 2 to project
		promoteActivities("MAIN/A/task2", "MAIN/A");
		activityRepository.save(activity("MAIN/A", "MAIN/A/task2", ActivityType.PROMOTION));

		// Create task 3
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A/task3", null, ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("300")
								.addComponentChange(new ComponentChange("300", ChangeType.CREATE, ComponentType.CONCEPT, "", true)))
		));

		// Run summary report for rebase only
		final ChangeSummaryReport rebaseReportOnTaskBeforeVersioning = reportService.createChangeSummaryReport("MAIN/A/task3", null, null, false, false, true);
		// Previous promoted changes to MAIN and MAIN/A should be found in the rebase report
		assertEquals("{CONCEPT=[100, 200]}", toString(rebaseReportOnTaskBeforeVersioning.getComponentChanges()));

		// Versioning on MAIN
		activityRepository.save(activity("MAIN", null, ActivityType.CREATE_CODE_SYSTEM_VERSION));

		// Before rebasing
		final ChangeSummaryReport rebaseReportOnTaskAfterVersioning = reportService.createChangeSummaryReport("MAIN/A/task3", null, null, false, false, true);
		// Only changes promoted MAIN/A after versioning should be found in the rebase report
		assertEquals("{CONCEPT=[100, 200]}", toString(rebaseReportOnTaskAfterVersioning.getComponentChanges()));


		// After rebasing MAIN
		activityRepository.save(activity("MAIN/A", "MAIN", ActivityType.REBASE));
		activityRepository.save(activity("MAIN/A/task3", "MAIN/A", ActivityType.REBASE));

		final ChangeSummaryReport reportOnTaskAfterVersioningAndRebasing = reportService.createChangeSummaryReport("MAIN/A/task3", null, null, false, false, true);
		// Only changes promoted MAIN/A and not versioned should in the rebase report
		assertEquals("{CONCEPT=[200]}", toString(reportOnTaskAfterVersioningAndRebasing.getComponentChanges()));
	}

	@Test
	void testChangeMadeDirectlyOnCodeSystemBranch() {
		// Create refset member on CodeSystem branch
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/SNOMEDCT-A", null, ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("a1", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, "", true)))
		));

		// Create a task on project and delete the member
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/SNOMEDCT-A/project/task", null, ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("a1", ChangeType.DELETE, ComponentType.REFERENCE_SET_MEMBER, "", true)))
		));

		// Promote task to project and to CodeSystemBranch
		promoteActivities("MAIN/SNOMEDCT-A/project/task", "MAIN/SNOMEDCT-A/project");
		promoteActivities("MAIN/SNOMEDCT-A/project", "MAIN/SNOMEDCT-A");

		// Run report on project and the refset member should be deleted.
		ChangeSummaryReport projectReport = reportService.createChangeSummaryReport("MAIN/SNOMEDCT-A/project");
		assertTrue(projectReport.getComponentChanges().isEmpty());

		// Add the same component back on the codeSystem branch again
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/SNOMEDCT-A", null, ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("a1", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, "", true)))
		));

		projectReport = reportService.createChangeSummaryReport("MAIN/SNOMEDCT-A/project");
		assertFalse(projectReport.getComponentChanges().isEmpty());
		assertEquals("{REFERENCE_SET_MEMBER=[a1]}", toString(projectReport.getComponentChanges()));
	}

	@Test
	void testReportWithSupersededChanges() {
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A/A1", "", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("110", ChangeType.UPDATE, ComponentType.DESCRIPTION, "", true))
						),
				activity("MAIN/A/A2", "", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("110", ChangeType.DELETE, ComponentType.DESCRIPTION, "", true))
						),
				activity("MAIN/A/A3", "", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("103")
								.addComponentChange(new ComponentChange("113", ChangeType.CREATE, ComponentType.DESCRIPTION, "", true))
						)
		));

		promoteActivities("MAIN/A/A1", "MAIN/A");

		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A/A2", "MAIN/A", ActivityType.REBASE)
						// If the MAIN/A version is chosen during merge, it will log the component change on the target branch as superseded during the rebase
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("110", ChangeType.DELETE, ComponentType.DESCRIPTION, "", true, true))
						)
		));

		// Report changes on MAIN/A/A2 only now should be empty as superseded.
		ChangeSummaryReport changeSummaryReport = reportService.createChangeSummaryReport("MAIN/A/A2", null, null, true, true, false);
		assertEquals(0, changeSummaryReport.getComponentChanges().size());
		assertEquals("{}", toString(changeSummaryReport.getComponentChanges()));

		// Report on MAIN/A should have changes promoted from MAIN/A/A1
		changeSummaryReport = reportService.createChangeSummaryReport("MAIN/A");
		assertEquals(1, changeSummaryReport.getComponentChanges().size());
		assertEquals("{DESCRIPTION=[110]}", toString(changeSummaryReport.getComponentChanges()));

		// Run summary report on MAIN/A/A2 should have contained changes rebased from MAIN/A
		changeSummaryReport = reportService.createChangeSummaryReport("MAIN/A/A2");
		assertEquals(1, changeSummaryReport.getComponentChanges().size());
		assertEquals("{DESCRIPTION=[110]}", toString(changeSummaryReport.getComponentChanges()));

		// The report on MAIN/A should have still one change after promoting MAIN/A/A2
		promoteActivities("MAIN/A/A2", "MAIN/A");
		changeSummaryReport = reportService.createChangeSummaryReport("MAIN/A");
		assertEquals(1, changeSummaryReport.getComponentChanges().size());
		assertEquals("{DESCRIPTION=[110]}", toString(changeSummaryReport.getComponentChanges()));

		// Promote MAIN/A/A3 to MAIN/A
		promoteActivities("MAIN/A/A3", "MAIN/A");
		// Promote MAIN/A to MAIN
		promoteActivities("MAIN/A", "MAIN");

		// Run summary report on MAIN and should contain two changes
		changeSummaryReport = reportService.createChangeSummaryReport("MAIN");
		assertEquals(1, changeSummaryReport.getComponentChanges().size());
		assertEquals("{DESCRIPTION=[110, 113]}", toString(changeSummaryReport.getComponentChanges()));
	}


	@Test
	void testReportWithSupersededChangeReverted() {
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A/A1", "", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("110", ChangeType.UPDATE, ComponentType.DESCRIPTION, "", true))
						),
				activity("MAIN/A/A2", "", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("110", ChangeType.DELETE, ComponentType.DESCRIPTION, "", true))
						)
		));
		promoteActivities("MAIN/A/A1", "MAIN/A");

		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A/A2", "MAIN/A", ActivityType.REBASE)
						// If the MAIN/A version is chosen during merge, it will log the component change on the target branch as superseded during the rebase
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("110", ChangeType.DELETE, ComponentType.DESCRIPTION, "", true, true))
						)
		));
		// Delete after rebase to correct rebase mistake.
		// Test case for superseded changes apply to prior changes only
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A/A2", "", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("110", ChangeType.DELETE, ComponentType.DESCRIPTION, "", true))
						)
		));

		// Run summary report on MAIN/A/A2 and should contain no change
		ChangeSummaryReport changeSummaryReport = reportService.createChangeSummaryReport("MAIN/A/A2");
		assertEquals(0, changeSummaryReport.getComponentChanges().size());
		assertEquals("{}", toString(changeSummaryReport.getComponentChanges()));
		}


	@Test
	void testReportWithSupersededChangeDuringProjectRebase() {
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A/A1", "", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("110", ChangeType.UPDATE, ComponentType.DESCRIPTION, "", true))
						)
		));
		promoteActivities("MAIN/A/A1", "MAIN/A");

		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A", "MAIN", ActivityType.REBASE)
						// Choose the version from MAIN to make change promoted from A1 to be superseded
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("110", ChangeType.UPDATE, ComponentType.DESCRIPTION, "", true, true))
						)
		));

		ChangeSummaryReport changeSummaryReport = reportService.createChangeSummaryReport("MAIN/A");
		assertEquals(0, changeSummaryReport.getComponentChanges().size());
		assertEquals("{}", toString(changeSummaryReport.getComponentChanges()));

		promoteActivities("MAIN/A", "MAIN");

		// Run summary report on MAIN and should contain no change
		changeSummaryReport = reportService.createChangeSummaryReport("MAIN");
		assertEquals(0, changeSummaryReport.getComponentChanges().size());
		assertEquals("{}", toString(changeSummaryReport.getComponentChanges()));
	}

	@Test
	void testSummaryReportOnProjectWithConflictChanges() {
		// Create a relationship on project during classification save
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A", null, ActivityType.CLASSIFICATION_SAVE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("R1", ChangeType.CREATE, ComponentType.RELATIONSHIP, "", true)))
		));

		// Create a task on project and delete the relationship
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A/TaskA", null, ActivityType.CLASSIFICATION_SAVE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("R1", ChangeType.DELETE, ComponentType.RELATIONSHIP, "", true)))
		));

		// Promote task to project
		promoteActivities("MAIN/A/TaskA", "MAIN/A");

		// Run report on project and the relationship should be deleted.
		ChangeSummaryReport projectReport = reportService.createChangeSummaryReport("MAIN/A");
		assertTrue(projectReport.getComponentChanges().isEmpty());
	}


	@Test
	void testRebaseChangesWhenDeletingAfterCreating() {
		// CodeSystem creates Relationship
		activityRepository.save(activity("MAIN/SNOMEDCT-TEST", null, ActivityType.CONTENT_CHANGE).addConceptChange(new ConceptChange("100").addComponentChange(new ComponentChange("120", ChangeType.CREATE, ComponentType.RELATIONSHIP, "", true))));

		// Rebase Project onto CodeSystem
		activityRepository.save(activity("MAIN/SNOMEDCT-TEST/project", "MAIN/SNOMEDCT-TEST",  ActivityType.REBASE));

		// Project deletes Relationship
		activityRepository.save(activity("MAIN/SNOMEDCT-TEST/project", null, ActivityType.CONTENT_CHANGE).addConceptChange(new ConceptChange("100").addComponentChange(new ComponentChange("120", ChangeType.DELETE, ComponentType.RELATIONSHIP, "", true))));

		// Rebase Task onto Project
		activityRepository.save(activity("MAIN/SNOMEDCT-TEST/project/task", "MAIN/SNOMEDCT-TEST/project",  ActivityType.REBASE));

		// Assert summary report on task does not contain relationship changes
		// Rebased changes only
		Map<ComponentType, Set<String>> componentChanges = reportService.createChangeSummaryReport("MAIN/SNOMEDCT-TEST/project/task", null,null, false, false, true).getComponentChanges();
		assertTrue(componentChanges.isEmpty());

		// All types
		componentChanges = reportService.createChangeSummaryReport("MAIN/SNOMEDCT-TEST/project/task", null,null).getComponentChanges();
		assertTrue(componentChanges.isEmpty());
	}


	@Test
	void testReportOnTaskWithParentProjectHavingNewCommits() throws Exception {
		// Create Task A and use current as the base time
		final long taskABaseTime = System.currentTimeMillis();
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/project", "", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("100", ChangeType.CREATE, ComponentType.CONCEPT, "", true))
						)
		));

		// Create a new concept on task B and promote to project
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/project", "MAIN/project/taskB", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("200")
								.addComponentChange(new ComponentChange("200", ChangeType.CREATE, ComponentType.CONCEPT, "", true))
						)
		));
		promoteActivities("MAIN/project/taskB", "MAIN/project");

		// Run report on task A. It shouldn't have concept change from task B
		Map<ComponentType, Set<String>> componentChanges = reportService.createChangeSummaryReport("MAIN/project/taskA", taskABaseTime,null).getComponentChanges();
		assertFalse(componentChanges.isEmpty());
		Set<String> conceptIds = componentChanges.get(ComponentType.CONCEPT);
		assertEquals(1, conceptIds.size());
		assertTrue(conceptIds.contains("100"));
	}

	@Test
	void testReportOnTaskWithNewParentProjectPromotionToMain() throws Exception {
		// Create a concept on task B
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/project/taskB", "", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("100", ChangeType.CREATE, ComponentType.CONCEPT, "", true))
						)
		));

		// Promote taskB to project
		promoteActivities("MAIN/project/taskB", "MAIN/project");
		activityRepository.save(activity("MAIN/project", "MAIN/project/taskB", ActivityType.PROMOTION));

		// Create Task A and use current as the base time
		final long taskABaseTime = System.currentTimeMillis();
		// Promote project to MAIN
		promoteActivities("MAIN/project", "MAIN");
		activityRepository.save(activity("MAIN", "MAIN/project", ActivityType.PROMOTION));

		// Run report on task A. It should still have concept change from project
		// but because the highest promoted branch is now MAIN and the promotionDate is later than taskA's base time
		Map<ComponentType, Set<String>> componentChanges = reportService.createChangeSummaryReport("MAIN/project/taskA", taskABaseTime-1,null).getComponentChanges();
		assertTrue(componentChanges.isEmpty());

		// Rebase task A will get the change promoted to MAIN
		// Simulate rebase
		final long taskABaseTimeAfterRebase = System.currentTimeMillis();
		componentChanges = reportService.createChangeSummaryReport("MAIN/project/taskA", taskABaseTimeAfterRebase-1,null).getComponentChanges();
		assertFalse(componentChanges.isEmpty());
		Set<String> conceptIds = componentChanges.get(ComponentType.CONCEPT);
		assertEquals(1, conceptIds.size());
		assertTrue(conceptIds.contains("100"));
	}

	@Test
	void testReportOnTaskWithNewPromotionToMainFromOtherProject() throws Exception {

		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/projectA", "", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("100", ChangeType.CREATE, ComponentType.CONCEPT, "", true))
						)
		));

		// Create Task A and use current as the base time
		final long taskABaseTime = System.currentTimeMillis();

		// Create a new concept on task B in project B and promote to MAIN
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN", "MAIN/projectB/taskB", ActivityType.PROMOTION),
				activity("MAIN", "MAIN/projectB/taskB", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("200")
								.addComponentChange(new ComponentChange("200", ChangeType.CREATE, ComponentType.CONCEPT, "", true))
						)
		));

		// Run report on task A. It shouldn't have concept change from project B
		Map<ComponentType, Set<String>> componentChanges = reportService.createChangeSummaryReport("MAIN/projectA/taskA", taskABaseTime-1,null).getComponentChanges();
		assertFalse(componentChanges.isEmpty());
		Set<String> conceptIds = componentChanges.get(ComponentType.CONCEPT);
		assertEquals(1, conceptIds.size());
		assertTrue(conceptIds.contains("100"));
	}

	@Test
	void testReportOnTaskWithNewProjectRebasingFromMain() {

		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/projectA", "", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("100", ChangeType.CREATE, ComponentType.CONCEPT, "", true))
						)
		));

		// Create Task A and use current as the base time
		final long taskABaseTime = System.currentTimeMillis();

		// Create a new concept on task B in project B and promote to MAIN
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN", "MAIN/projectB/taskB", ActivityType.PROMOTION),
				activity("MAIN", "MAIN/projectB/taskB", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("200")
								.addComponentChange(new ComponentChange("200", ChangeType.CREATE, ComponentType.CONCEPT, "", true))
						)
		));

		// Rebase project from MAIN but not task
		activityRepository.save(activity("MAIN/projectA", "MAIN", ActivityType.REBASE));

		// Run report on task A. It shouldn't have concept change for 200 promoted to MAIN
		Map<ComponentType, Set<String>> componentChanges = reportService.createChangeSummaryReport("MAIN/projectA/taskA", taskABaseTime,null).getComponentChanges();
		assertFalse(componentChanges.isEmpty());
		Set<String> conceptIds = componentChanges.get(ComponentType.CONCEPT);
		assertEquals(1, conceptIds.size());
		assertTrue(conceptIds.contains("100"));

		// Rebase task it should have two concepts
		activityRepository.save(activity("MAIN/projectA/taskA", "MAIN/projectA", ActivityType.REBASE));
		componentChanges = reportService.createChangeSummaryReport("MAIN/projectA/taskA", System.currentTimeMillis(),null).getComponentChanges();
		assertFalse(componentChanges.isEmpty());
		conceptIds = componentChanges.get(ComponentType.CONCEPT);
		assertEquals(2, conceptIds.size());
		assertTrue(conceptIds.contains("200"));
	}


	@Test
	void testReportOnTaskWithConflictChangeInProjectAndMain() {
		// Inactivate a relationship on task A during classification save
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A/taskA", null, ActivityType.CLASSIFICATION_SAVE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("R1", ChangeType.INACTIVATE, ComponentType.RELATIONSHIP, "", true)))
		));

		// Promote task to project
		promoteActivities("MAIN/A/TaskA", "MAIN/A");

		// Promote project A to MAIN
		promoteActivities("MAIN/A", "MAIN");

		// Create task B on project A and restore published state for above relationship
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A/TaskB", null, ActivityType.CLASSIFICATION_SAVE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("R1", ChangeType.UPDATE, ComponentType.RELATIONSHIP, "", false)))
		));

		// Promote taskB to project
		promoteActivities("MAIN/A/TaskB", "MAIN/A");

		// Create a new taskC on project A and run report
		ChangeSummaryReport projectReport = reportService.createChangeSummaryReport("MAIN/A/TaskC");
		assertTrue(projectReport.getComponentChanges().isEmpty());
	}

	@Test
	void testIncludeMadeOnTaskOnly() {
		final long taskABaseTime = System.currentTimeMillis();
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A/TaskA", "", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("100", ChangeType.CREATE, ComponentType.CONCEPT, "", true))
						)
		));

		// Without date range
		ChangeSummaryReport taskReport = reportService.createChangeSummaryReport("MAIN/A/TaskA", null, null,
				true, false, false);
		assertNotNull(taskReport.getComponentChanges().get(ComponentType.CONCEPT));
		assertTrue(taskReport.getComponentChanges().get(ComponentType.CONCEPT).contains("100"));

		// With contentBaseTimeStamp only
		// Empty results
		taskReport = reportService.createChangeSummaryReport("MAIN/A/TaskA", taskABaseTime-10, null,
				true, false, false);
		assertNotNull(taskReport.getComponentChanges().get(ComponentType.CONCEPT));
		assertTrue(taskReport.getComponentChanges().get(ComponentType.CONCEPT).contains("100"));

		// having no results
		taskReport = reportService.createChangeSummaryReport("MAIN/A/TaskA", System.currentTimeMillis(), null,
				true, false, false);
		assertTrue(taskReport.getComponentChanges().isEmpty());

		// with contentHeadTimestamp only
		taskReport = reportService.createChangeSummaryReport("MAIN/A/TaskA", null, System.currentTimeMillis(),
				true, false, false);
		assertNotNull(taskReport.getComponentChanges().get(ComponentType.CONCEPT));
		assertTrue(taskReport.getComponentChanges().get(ComponentType.CONCEPT).contains("100"));

		// having no results
		taskReport = reportService.createChangeSummaryReport("MAIN/A/TaskA", null, taskABaseTime-1000,
				true, false, false);
		assertTrue(taskReport.getComponentChanges().isEmpty());

		// with contentBaseTimeStamp and contentHeadTimestamp
		taskReport = reportService.createChangeSummaryReport("MAIN/A/TaskA", taskABaseTime-10, System.currentTimeMillis(),
				true, false, false);
		assertNotNull(taskReport.getComponentChanges().get(ComponentType.CONCEPT));
		assertTrue(taskReport.getComponentChanges().get(ComponentType.CONCEPT).contains("100"));
	}

	@Test
	void testIncludeMadeOnCodeSystemBranchOnly() {
		// Changes made directly on a Code System branch is rare and not recommended
		// However it can happen for classification save
		final long baseTime = System.currentTimeMillis();
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN", "", ActivityType.CLASSIFICATION_SAVE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("10001", ChangeType.CREATE, ComponentType.RELATIONSHIP, "", true))
						)
		));

		// Without date range
		ChangeSummaryReport summaryReport = reportService.createChangeSummaryReport("MAIN", null, null,
				true, false, false);
		assertNotNull(summaryReport.getComponentChanges().get(ComponentType.RELATIONSHIP));
		assertEquals(1, summaryReport.getComponentChanges().get(ComponentType.RELATIONSHIP).size());
		assertTrue(summaryReport.getComponentChanges().get(ComponentType.RELATIONSHIP).contains("10001"));

		// With contentBaseTimeStamp
		summaryReport = reportService.createChangeSummaryReport("MAIN", baseTime-10, null,
				true, false, false);
		assertNotNull(summaryReport.getComponentChanges().get(ComponentType.RELATIONSHIP));
		assertEquals(1, summaryReport.getComponentChanges().get(ComponentType.RELATIONSHIP).size());
		assertTrue(summaryReport.getComponentChanges().get(ComponentType.RELATIONSHIP).contains("10001"));
		// Empty results
		summaryReport = reportService.createChangeSummaryReport("MAIN", System.currentTimeMillis(), null,
				true, false, false);

		// With contentHeadTimestamp
		summaryReport = reportService.createChangeSummaryReport("MAIN", null, System.currentTimeMillis(),
				true, false, false);
		assertNotNull(summaryReport.getComponentChanges().get(ComponentType.RELATIONSHIP));
		assertEquals(1, summaryReport.getComponentChanges().get(ComponentType.RELATIONSHIP).size());
		assertTrue(summaryReport.getComponentChanges().get(ComponentType.RELATIONSHIP).contains("10001"));
		// Empty results
		summaryReport = reportService.createChangeSummaryReport("MAIN", null, baseTime-100,
				true, false, false);
		assertTrue(summaryReport.getComponentChanges().isEmpty());

		// With contentBaseTimeStamp and contentHeadTimestamp
		summaryReport = reportService.createChangeSummaryReport("MAIN", baseTime-10, System.currentTimeMillis(),
				true, false, false);
		assertNotNull(summaryReport.getComponentChanges().get(ComponentType.RELATIONSHIP));
		assertEquals(1, summaryReport.getComponentChanges().get(ComponentType.RELATIONSHIP).size());
		assertTrue(summaryReport.getComponentChanges().get(ComponentType.RELATIONSHIP).contains("10001"));

		// Versioning
		activityRepository.save(activity("MAIN", null, ActivityType.CREATE_CODE_SYSTEM_VERSION));
		// Empty results
		summaryReport = reportService.createChangeSummaryReport("MAIN", null, System.currentTimeMillis(),
				true, false, false);
		assertTrue(summaryReport.getComponentChanges().isEmpty());

		summaryReport = reportService.createChangeSummaryReport("MAIN", baseTime, System.currentTimeMillis(),
				true, false, false);
		assertTrue(summaryReport.getComponentChanges().isEmpty());
	}
	@Test
	void testIncludePromotedToBranchOnly() {
		final long taskABaseTime = System.currentTimeMillis();
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A/TaskA", "", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("100")
								.addComponentChange(new ComponentChange("100", ChangeType.CREATE, ComponentType.CONCEPT, "", true))
						)
		));
		promoteActivities("MAIN/A/TaskA", "MAIN/A");
		final String projectA = "MAIN/A";

		final long taskBBaseTime = System.currentTimeMillis();
		activityRepository.saveAll(Lists.newArrayList(
				activity("MAIN/A/TaskB", "", ActivityType.CONTENT_CHANGE)
						.addConceptChange(new ConceptChange("200")
								.addComponentChange(new ComponentChange("200", ChangeType.CREATE, ComponentType.CONCEPT, "", true))
						)
		));
		promoteActivities("MAIN/A/TaskB", "MAIN/A");
		// Without date range
		ChangeSummaryReport summaryReport = reportService.createChangeSummaryReport(projectA, null, null,
				false, true, false);
		assertNotNull(summaryReport.getComponentChanges().get(ComponentType.CONCEPT));
		assertEquals(2, summaryReport.getComponentChanges().get(ComponentType.CONCEPT).size());
		assertTrue(summaryReport.getComponentChanges().get(ComponentType.CONCEPT).contains("100"));
		assertTrue(summaryReport.getComponentChanges().get(ComponentType.CONCEPT).contains("200"));

		// Use taskA as contentBaseTimestamp
		summaryReport = reportService.createChangeSummaryReport(projectA, taskABaseTime-1, null,
				false, true, false);
		assertEquals(2, summaryReport.getComponentChanges().get(ComponentType.CONCEPT).size());
		assertNotNull(summaryReport.getComponentChanges().get(ComponentType.CONCEPT));
		assertTrue(summaryReport.getComponentChanges().get(ComponentType.CONCEPT).contains("100"));
		assertTrue(summaryReport.getComponentChanges().get(ComponentType.CONCEPT).contains("200"));

		// Use taskB as contentBaseTimestamp
		summaryReport = reportService.createChangeSummaryReport(projectA, taskBBaseTime-1, null,
				false, true, false);
		assertEquals(1, summaryReport.getComponentChanges().get(ComponentType.CONCEPT).size());
		assertNotNull(summaryReport.getComponentChanges().get(ComponentType.CONCEPT));
		assertTrue(summaryReport.getComponentChanges().get(ComponentType.CONCEPT).contains("200"));

		// Empty results for current time as base time
		summaryReport = reportService.createChangeSummaryReport(projectA, System.currentTimeMillis(), null,
				false, true, false);
		assertTrue(summaryReport.getComponentChanges().isEmpty());

		// With current time as contentHeadTimestamp
		summaryReport = reportService.createChangeSummaryReport(projectA, null, System.currentTimeMillis(),
				false, true, false);
		assertNotNull(summaryReport.getComponentChanges().get(ComponentType.CONCEPT));
		assertEquals(2, summaryReport.getComponentChanges().get(ComponentType.CONCEPT).size());
		// Empty results with base time as contentHeadTime
		summaryReport = reportService.createChangeSummaryReport(projectA, null, taskABaseTime,
				false, true, false);
		assertTrue(summaryReport.getComponentChanges().isEmpty());

		// With taskABaseTime as contentBaseTimestamp and current time as the contentHeadTimestamp
		summaryReport = reportService.createChangeSummaryReport(projectA, taskABaseTime, System.currentTimeMillis(),
				false, true, false);
		assertNotNull(summaryReport.getComponentChanges().get(ComponentType.CONCEPT));
		assertEquals(2, summaryReport.getComponentChanges().get(ComponentType.CONCEPT).size());
		// Empty results with future time as base time
		long now = System.currentTimeMillis();
		summaryReport = reportService.createChangeSummaryReport(projectA, now, now+1000,
				false, true, false);
		assertTrue(summaryReport.getComponentChanges().isEmpty());

		// Promoting to MAIN
		promoteActivities("MAIN/A", "MAIN");

		// Running report on porject after promoting to MAIN
		summaryReport = reportService.createChangeSummaryReport(projectA, taskABaseTime, System.currentTimeMillis(),
				false, true, false);
		// The report is empty because all changes have promoted to parent branch
		assertTrue(summaryReport.getComponentChanges().isEmpty());

		// Running report on MAIN with contentBaseTimestamp and contentHeadTimestamp
		summaryReport = reportService.createChangeSummaryReport("MAIN", taskABaseTime, System.currentTimeMillis(),
				false, true, false);
		assertEquals(2, summaryReport.getComponentChanges().get(ComponentType.CONCEPT).size());
		// Empty results with future time as base time
		now = System.currentTimeMillis();
		summaryReport = reportService.createChangeSummaryReport("MAIN", now, now+1000,
				false, true, false);
		assertTrue(summaryReport.getComponentChanges().isEmpty());

		// Versioning
		activityRepository.save(activity("MAIN", null, ActivityType.CREATE_CODE_SYSTEM_VERSION));
		// Empty results after versioning
		summaryReport = reportService.createChangeSummaryReport("MAIN", taskABaseTime, System.currentTimeMillis(),
				false, true, false);
		assertTrue(summaryReport.getComponentChanges().isEmpty());
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

	private void promoteActivities(String task, String project) {
		// Move activities on the source branch up to the parent
		final List<ActivityType> contentActivityTypes = Lists.newArrayList(ActivityType.CLASSIFICATION_SAVE, ActivityType.CONTENT_CHANGE, ActivityType.REBASE);

		List<Activity> toSave = new ArrayList<>();
		try (final SearchHitsIterator<Activity> stream = elasticsearchOperations.searchForStream(new NativeSearchQueryBuilder()
				.withQuery(boolQuery()
						.must(termQuery(Activity.Fields.highestPromotedBranch, task))
						.must(termsQuery(Activity.Fields.activityType, contentActivityTypes)))
				.withPageable(PageRequest.of(0, 1_000)).build(), Activity.class)) {
			stream.forEachRemaining(activitySearchHit -> {
				final Activity activityToUpdate = activitySearchHit.getContent();
				activityToUpdate.setHighestPromotedBranch(project);
				activityToUpdate.setPromotionDate(new Date());
				toSave.add(activityToUpdate);
			});
			if (!toSave.isEmpty()) {
				toSave.forEach(activityRepository::save);
			}
		}
	}
}