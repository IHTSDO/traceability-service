package org.ihtsdo.otf.traceabilityservice;

import org.ihtsdo.otf.traceabilityservice.domain.*;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TraceabilityStreamConsumerTest extends AbstractTest {

	@Test
	void consumeConceptCreateAndPromoteTest() throws IOException, InterruptedException {
		List<Activity> activities = sendAndReceiveActivity("concept-create.json");

		assertEquals(1, activities.size());
		Activity activity = activities.get(0);
		assertEquals("kkewley", activity.getUsername());
		assertEquals("MAIN/STORMTEST2/STORMTEST2-243", activity.getBranch());
		assertEquals(3, activity.getBranchDepth());
		assertEquals(ActivityType.CONTENT_CHANGE, activity.getActivityType());
		final Set<ConceptChange> conceptChanges = activity.getConceptChanges();
		assertEquals(1, conceptChanges.size());
		final ConceptChange conceptChange = conceptChanges.iterator().next();
		assertEquals("4195653005", conceptChange.getConceptId().toString());
		final Set<ComponentChange> componentChanges = conceptChange.getComponentChanges();
		assertEquals(8, componentChanges.size());
		assertTrue(componentChanges.contains(new ComponentChange("4195653005", ChangeType.CREATE, ComponentType.CONCEPT, null)));
		assertTrue(componentChanges.contains(new ComponentChange("10430314015", ChangeType.CREATE, ComponentType.DESCRIPTION, Concepts.FSN)));
		assertTrue(componentChanges.contains(new ComponentChange("10430313014", ChangeType.CREATE, ComponentType.DESCRIPTION, Concepts.SYNONYM)));
		assertTrue(componentChanges.contains(new ComponentChange("3c3a1d55-2822-4577-98ea-0c286ae75b2d", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, Concepts.OWL_AXIOM_REFSET)));
		assertTrue(componentChanges.contains(new ComponentChange("b8fe9249-e532-4e34-b1c1-a12bb5fbfb66", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, Concepts.GB_LANG_REFSET)));
		assertTrue(componentChanges.contains(new ComponentChange("09918c72-ac79-4617-8ad1-cad4347831d0", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, Concepts.US_LANG_REFSET)));
		assertTrue(componentChanges.contains(new ComponentChange("2aee3da1-08fe-4ca7-827f-252c74d4aec5", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, Concepts.GB_LANG_REFSET)));
		assertTrue(componentChanges.contains(new ComponentChange("a68cf3f2-fedb-4a07-ba40-cd00f26c86b5", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, Concepts.US_LANG_REFSET)));

		activities = sendAndReceiveActivity("promotion.json");
		assertEquals(1, activities.size());
		activity = activities.get(0);
		assertEquals("kkewley", activity.getUsername());
		assertEquals("MAIN/STORMTEST2", activity.getBranch());
		assertEquals("MAIN/STORMTEST2/STORMTEST2-243", activity.getSourceBranch());
		assertEquals(ActivityType.PROMOTION, activity.getActivityType());
		assertEquals(0, activity.getConceptChanges().size());

		final Page<Activity> byBranch = activityRepository.findByBranch("MAIN/STORMTEST2/STORMTEST2-243", Pageable.unpaged());
		assertEquals(1, byBranch.getTotalElements());
		final Activity originalCommit = byBranch.getContent().iterator().next();
		assertEquals(ActivityType.CONTENT_CHANGE, originalCommit.getActivityType());
		assertEquals("MAIN/STORMTEST2", originalCommit.getHighestPromotedBranch());
	}

	@Test
	void consumeDescriptionDeleteTest() throws IOException, InterruptedException {
		final String resource = "description-delete.json";
		final List<Activity> activities = sendAndReceiveActivity(resource);

		assertEquals(1, activities.size());
		final Activity activity = activities.get(0);
		assertEquals("kkewley", activity.getUsername());
		assertEquals("MAIN/STORMTEST2/STORMTEST2-243", activity.getBranch());
		assertEquals(ActivityType.CONTENT_CHANGE, activity.getActivityType());
		final Set<ConceptChange> conceptChanges = activity.getConceptChanges();
		assertEquals(1, conceptChanges.size());
		final ConceptChange conceptChange = conceptChanges.iterator().next();
		assertEquals("4195653005", conceptChange.getConceptId().toString());
		final Set<ComponentChange> componentChanges = conceptChange.getComponentChanges();

		assertEquals(3, componentChanges.size());
		assertTrue(componentChanges.contains(new ComponentChange("10430313014", ChangeType.DELETE, ComponentType.DESCRIPTION, Concepts.SYNONYM)));
		assertTrue(componentChanges.contains(new ComponentChange("09918c72-ac79-4617-8ad1-cad4347831d0", ChangeType.DELETE, ComponentType.REFERENCE_SET_MEMBER, Concepts.US_LANG_REFSET)));
		assertTrue(componentChanges.contains(new ComponentChange("b8fe9249-e532-4e34-b1c1-a12bb5fbfb66", ChangeType.DELETE, ComponentType.REFERENCE_SET_MEMBER, Concepts.GB_LANG_REFSET)));
	}

	// description replacement
	// description reactivation
	// concept inactivation
	// concept delete
	// classification - create, inactivate, delete
	// large inactivation
	// rebase
	// promote
	// find by concept id

	// consume invalid message

}
