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
		final ChangeSummaryReport projectReportAfterVersioningAfterRebase = reportService.createChangeSummaryReport("MAIN/A", false, true, false);
		// Changes should be found
		assertEquals("{CONCEPT=[100]}", toString(projectReportAfterVersioningAfterRebase.getComponentChanges()));

		// Promote project to MAIN
		promoteActivities("MAIN/A", "MAIN");
		activityRepository.save(activity("MAIN/A", "MAIN", ActivityType.PROMOTION));

		// Run summary report on MAIN
		final ChangeSummaryReport reportOnMain = reportService.createChangeSummaryReport("MAIN", false, true, false);
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

		final ChangeSummaryReport promotionReportOnProject = reportService.createChangeSummaryReport("MAIN/A", false, true, false);
		// Promotion changes should be found in project
		assertEquals("{CONCEPT=[200]}", toString(promotionReportOnProject.getComponentChanges()));

		final ChangeSummaryReport rebaseReportOnProject = reportService.createChangeSummaryReport("MAIN/A", false, false, true);
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
		final ChangeSummaryReport rebaseReportOnTaskBeforeVersioning = reportService.createChangeSummaryReport("MAIN/A/task3", false, false, true);
		// Previous promoted changes to MAIN and MAIN/A should be found in the rebase report
		assertEquals("{CONCEPT=[100, 200]}", toString(rebaseReportOnTaskBeforeVersioning.getComponentChanges()));

		// Versioning on MAIN
		activityRepository.save(activity("MAIN", null, ActivityType.CREATE_CODE_SYSTEM_VERSION));

		// Before rebasing
		final ChangeSummaryReport rebaseReportOnTaskAfterVersioning = reportService.createChangeSummaryReport("MAIN/A/task3", false, false, true);
		// Only changes promoted MAIN/A after versioning should be found in the rebase report
		assertEquals("{CONCEPT=[100, 200]}", toString(rebaseReportOnTaskAfterVersioning.getComponentChanges()));


		// After rebasing MAIN
		activityRepository.save(activity("MAIN/A", "MAIN", ActivityType.REBASE));
		activityRepository.save(activity("MAIN/A/task3", "MAIN/A", ActivityType.REBASE));

		final ChangeSummaryReport reportOnTaskAfterVersioningAndRebasing = reportService.createChangeSummaryReport("MAIN/A/task3", false, false, true);
		// Only changes promoted MAIN/A and not versioned should in the rebase report
		assertEquals("{CONCEPT=[200]}", toString(reportOnTaskAfterVersioningAndRebasing.getComponentChanges()));
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