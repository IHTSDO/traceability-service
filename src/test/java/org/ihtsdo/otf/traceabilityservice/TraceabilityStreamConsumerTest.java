package org.ihtsdo.otf.traceabilityservice;

import org.ihtsdo.otf.traceabilityservice.domain.*;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.util.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;


class TraceabilityStreamConsumerTest extends AbstractTest {

	@Autowired
	private ActivityRepository activityRepository;

	@Autowired
	private JmsTemplate jmsTemplate;

	@Autowired
	private String destinationName;

	@BeforeEach
	void setup() {
		jmsTemplate.setDeliveryPersistent(false);
		activityRepository.deleteAll();
	}

	@Test
	void consumeConceptCreateTest() throws IOException, InterruptedException {
		final String resource = "concept-create.json";
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
		assertEquals(8, componentChanges.size());
		assertTrue(componentChanges.contains(new ComponentChange("4195653005", ChangeType.CREATE, ComponentType.CONCEPT, null)));
		assertTrue(componentChanges.contains(new ComponentChange("10430314015", ChangeType.CREATE, ComponentType.DESCRIPTION, Concepts.FSN)));
		assertTrue(componentChanges.contains(new ComponentChange("10430313014", ChangeType.CREATE, ComponentType.DESCRIPTION, Concepts.SYNONYM)));
		assertTrue(componentChanges.contains(new ComponentChange("3c3a1d55-2822-4577-98ea-0c286ae75b2d", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, Concepts.OWL_AXIOM_REFSET)));
		assertTrue(componentChanges.contains(new ComponentChange("b8fe9249-e532-4e34-b1c1-a12bb5fbfb66", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, Concepts.GB_LANG_REFSET)));
		assertTrue(componentChanges.contains(new ComponentChange("09918c72-ac79-4617-8ad1-cad4347831d0", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, Concepts.US_LANG_REFSET)));
		assertTrue(componentChanges.contains(new ComponentChange("2aee3da1-08fe-4ca7-827f-252c74d4aec5", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, Concepts.GB_LANG_REFSET)));
		assertTrue(componentChanges.contains(new ComponentChange("a68cf3f2-fedb-4a07-ba40-cd00f26c86b5", ChangeType.CREATE, ComponentType.REFERENCE_SET_MEMBER, Concepts.US_LANG_REFSET)));
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

	private PageRequest getPageRequestMax() {
		return PageRequest.of(0, 10_000);
	}

	private List<Activity> sendAndReceiveActivity(String resource) throws IOException, InterruptedException {
		long startingActivityCount = activityRepository.count();

		final InputStream resourceAsStream = getClass().getResourceAsStream(resource);
		assertNotNull(resourceAsStream);
		final String message = StreamUtils.copyToString(resourceAsStream, StandardCharsets.UTF_8);
		sendMessage(message);

		int timeoutSeconds = 10;
		int waitedSeconds = 0;
		while (activityRepository.count() != startingActivityCount + 1 && waitedSeconds < timeoutSeconds) {
			Thread.sleep(1000);
			waitedSeconds++;
		}

		final List<Activity> activities = new ArrayList<>();
		activityRepository.findAll().forEach(activities::add);
		assertNotNull(activities);
		return activities;
	}

	private Map<Long, ConceptChange> getConceptChangeMap(Set<ConceptChange> conceptChanges) {
		return conceptChanges.stream().collect(Collectors.toMap(ConceptChange::getConceptId, Function.identity()));
	}

	private void sendMessage(final String message) {
		MessageCreator messageCreator = session -> session.createTextMessage(message);
		jmsTemplate.send(destinationName, messageCreator);
	}

}
