package org.ihtsdo.otf.traceabilityservice;

import org.ihtsdo.otf.traceabilityservice.domain.*;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.ihtsdo.otf.traceabilityservice.repository.BranchRepository;
import org.ihtsdo.otf.traceabilityservice.setup.LogLoaderException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.util.FileSystemUtils;
import org.springframework.util.StreamUtils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ApplicationIntegrationTest {

	private ConfigurableApplicationContext context;
	private ActivityRepository activityRepository;
	private BranchRepository branchRepository;
	private JmsTemplate jmsTemplate;
	private String destinationName;

	@Before
	public void setup() throws LogLoaderException {
		// Clean out any ActiveMQ data from a previous run
		FileSystemUtils.deleteRecursively(new File("activemq-data"));
		Application.main(new String[]{});
		context = Application.getContext();
		activityRepository = context.getBean(ActivityRepository.class);
		branchRepository = context.getBean(BranchRepository.class);
		jmsTemplate = context.getBean(JmsTemplate.class);
		destinationName = context.getBeanFactory().resolveEmbeddedValue("${platform.name}." + Application.TRACEABILITY_QUEUE_SUFFIX);
	}

	@After
	public void tearDown() {
		context.close();
	}

	@Test
	public void consumeClassificationTest() throws IOException, InterruptedException {
		final String resource = "traceability-classification-save.txt";

		final ArrayList<Activity> activities = streamTestDataAndRetrievePersistedActivities(resource);

		assertEquals(1, activities.size());
		final Activity activity = activities.get(0);
		assertEquals("snowowl", activity.getUser().getUsername());
		assertEquals("MAIN/CONREQEXT/CONREQEXT-442", activity.getBranch().getBranchPath());
		assertEquals(ActivityType.CLASSIFICATION_SAVE, activity.getActivityType());
		final Set<ConceptChange> conceptChanges = activity.getConceptChanges();
		assertEquals(5, conceptChanges.size());
		final Map<Long, ConceptChange> conceptChangeMap = getConceptChangeMap(conceptChanges);
		final ConceptChange conceptChange = conceptChangeMap.get(426560005L);
		assertEquals("426560005", conceptChange.getConceptId().toString());
		final Set<ComponentChange> componentChanges = conceptChange.getComponentChanges();
		assertEquals(3, componentChanges.size());
		Assert.assertTrue(componentChanges.contains(new ComponentChange("6552672027", ComponentChangeType.CREATE, ComponentType.RELATIONSHIP, ComponentSubType.INFERRED_RELATIONSHIP)));
		Assert.assertTrue(componentChanges.contains(new ComponentChange("3207822025", ComponentChangeType.INACTIVATE, ComponentType.RELATIONSHIP, ComponentSubType.INFERRED_RELATIONSHIP)));
		Assert.assertTrue(componentChanges.contains(new ComponentChange("3198463025", ComponentChangeType.INACTIVATE, ComponentType.RELATIONSHIP, ComponentSubType.INFERRED_RELATIONSHIP)));
	}

	@Test
	public void consumeClassificationDelAndCreateTest() throws IOException, InterruptedException {
		final String resource = "traceability-classif-rel-del-create.txt";

		final ArrayList<Activity> activities = streamTestDataAndRetrievePersistedActivities(resource);

		assertEquals(1, activities.size());
		final Activity activity = activities.get(0);
		assertEquals(ActivityType.CLASSIFICATION_SAVE, activity.getActivityType());
	}

	@Test
	public void consumeClassificationRelationshipDeletionOnlyTest() throws IOException, InterruptedException {
		final String resource = "traceability-classif-rel-del-only.txt";

		final ArrayList<Activity> activities = streamTestDataAndRetrievePersistedActivities(resource);

		assertEquals(1, activities.size());
		final Activity activity = activities.get(0);
		assertEquals(ActivityType.CLASSIFICATION_SAVE, activity.getActivityType());
	}

	@Test
	public void consumeManualChangeRelationshipDeletionOnlyTest() throws IOException, InterruptedException {
		final String resource = "traceability-manual-rel-del-only.txt";

		final ArrayList<Activity> activities = streamTestDataAndRetrievePersistedActivities(resource);

		assertEquals(1, activities.size());
		final Activity activity = activities.get(0);
		assertEquals(ActivityType.CONTENT_CHANGE, activity.getActivityType());
	}

	@Test
	public void consumeConceptUpdateTest() throws IOException, InterruptedException {
		final String resource = "traceability-example-update.txt";

		final ArrayList<Activity> activities = streamTestDataAndRetrievePersistedActivities(resource);

		assertEquals(1, activities.size());
		final Activity activity = activities.get(0);
		assertEquals("snowowl", activity.getUser().getUsername());
		assertEquals("MAIN/CONREQEXT/CONREQEXT-442", activity.getBranch().getBranchPath());
		assertEquals(ActivityType.CONTENT_CHANGE, activity.getActivityType());
		final Set<ConceptChange> conceptChanges = activity.getConceptChanges();
		assertEquals(1, conceptChanges.size());
		final ConceptChange conceptChange = conceptChanges.iterator().next();
		assertEquals("416390003", conceptChange.getConceptId().toString());
		final Set<ComponentChange> componentChanges = conceptChange.getComponentChanges();
		assertEquals(4, componentChanges.size());
		Assert.assertTrue(componentChanges.contains(new ComponentChange("2546600013", ComponentChangeType.UPDATE, ComponentType.DESCRIPTION, ComponentSubType.FSN_DESCRIPTION)));
		Assert.assertTrue(componentChanges.contains(new ComponentChange("3305226012", ComponentChangeType.CREATE, ComponentType.DESCRIPTION, ComponentSubType.SYNONYM_DESCRIPTION)));
		Assert.assertTrue(componentChanges.contains(new ComponentChange("3305227015", ComponentChangeType.CREATE, ComponentType.DESCRIPTION, ComponentSubType.FSN_DESCRIPTION)));
		Assert.assertTrue(componentChanges.contains(new ComponentChange("41241d85-62be-437b-931a-eb036b126cc1", ComponentChangeType.CREATE, ComponentType.OWLAXIOM, null)));
	}

	@Test
	public void consumeConceptDeleteTest() throws IOException, InterruptedException {
		final String resource = "traceability-concept-deletion.txt";

		final ArrayList<Activity> activities = streamTestDataAndRetrievePersistedActivities(resource);

		assertEquals(1, activities.size());
		final Activity activity = activities.get(0);
		assertEquals("snowowl", activity.getUser().getUsername());
		assertEquals("MAIN/CONREQEXT/CONREQEXT-374", activity.getBranch().getBranchPath());
		assertEquals(ActivityType.CONTENT_CHANGE, activity.getActivityType());
		final Set<ConceptChange> conceptChanges = activity.getConceptChanges();
		assertEquals(1, conceptChanges.size());
		final ConceptChange conceptChange = conceptChanges.iterator().next();
		assertEquals("715891009", conceptChange.getConceptId().toString());
		final Set<ComponentChange> componentChanges = conceptChange.getComponentChanges();
		assertEquals(7, componentChanges.size());
		Assert.assertTrue(componentChanges.contains(new ComponentChange("715891009", ComponentChangeType.DELETE, ComponentType.CONCEPT, null)));
		Assert.assertTrue(componentChanges.contains(new ComponentChange("3302620017", ComponentChangeType.DELETE, ComponentType.DESCRIPTION, null)));
		Assert.assertTrue(componentChanges.contains(new ComponentChange("6546557027", ComponentChangeType.DELETE, ComponentType.RELATIONSHIP, null)));
	}

	@Test
	public void consumeBranchRebaseTest() throws IOException, InterruptedException {
		final String resource = "traceability-branch-rebase.txt";

		final ArrayList<Activity> activities = streamTestDataAndRetrievePersistedActivities(resource);

		assertEquals(1, activities.size());
		final Activity activity = activities.get(0);
		assertEquals("kkewley", activity.getUser().getUsername());
		assertEquals("MAIN/CMTFH/CMTFH-6", activity.getBranch().getBranchPath());
		assertEquals("MAIN/CMTFH", activity.getMergeSourceBranch().getBranchPath());
		assertEquals(ActivityType.REBASE, activity.getActivityType());
		final Set<ConceptChange> conceptChanges = activity.getConceptChanges();
		assertEquals(0, conceptChanges.size());
	}

	@Test
	public void consumeBranchPromoteTest() throws IOException, InterruptedException {
		final String resource = "traceability-branch-promote.txt";

		final ArrayList<Activity> activities = streamTestDataAndRetrievePersistedActivities(resource);

		assertEquals(1, activities.size());
		final Activity activity = activities.get(0);
		assertEquals("kkewley", activity.getUser().getUsername());
		assertEquals("MAIN/CMTFH", activity.getBranch().getBranchPath());
		assertEquals("MAIN/CMTFH/CMTFH-6", activity.getMergeSourceBranch().getBranchPath());
		assertEquals(ActivityType.PROMOTION, activity.getActivityType());
		final Set<ConceptChange> conceptChanges = activity.getConceptChanges();
		assertEquals(0, conceptChanges.size());
	}

	@Test
	public void consumeCreateRebasePromoteTest() throws IOException, InterruptedException {

		streamTestDataAndRetrievePersistedActivities("traceability-create-rebase.txt");

		final Branch projectBranch = branchRepository.findByBranchPath("MAIN/CMTFH");
		final List<Activity> activitiesAtProjectBeforePromotion = activityRepository.findByHighestPromotedBranchOrderByCommitDate(projectBranch);
		assertEquals(0, activitiesAtProjectBeforePromotion.size());

		streamTestDataAndRetrievePersistedActivities("traceability-branch-promote.txt");

		final List<Activity> activitiesAtProjectAfterPromotion = activityRepository.findByHighestPromotedBranchOrderByCommitDate(projectBranch);
		assertEquals(3, activitiesAtProjectAfterPromotion.size());
		final Activity activity = activitiesAtProjectAfterPromotion.get(0);
		assertEquals(ActivityType.CONTENT_CHANGE, activity.getActivityType());
		assertEquals("716755000", activity.getConceptChanges().iterator().next().getConceptId().toString());
	}

	@Test
	public void consumeDescriptionReplacementTest() throws IOException, InterruptedException {
		streamTestDataAndRetrievePersistedActivities("traceability-description-replacement.txt");

		final Branch branch = branchRepository.findByBranchPath("MAIN/DRUGSJAN19/DRUGSJAN19-1198");
		final List<Activity> activities = activityRepository.findByHighestPromotedBranchOrderByCommitDate(branch);
		assertEquals(1, activities.size());
		final Activity activity = activities.get(0);
		assertEquals(ActivityType.CONTENT_CHANGE, activity.getActivityType());
		Set<ConceptChange> conceptChanges = activity.getConceptChanges();
		// There are 2 changes in the payload at this level but only 1 is against a concept id
		assertEquals(1, conceptChanges.size());
		ConceptChange conceptChange = conceptChanges.iterator().next();
		assertEquals("781176006", conceptChange.getConceptId().toString());
		List<ComponentChange> componentChanges = new ArrayList<>(conceptChange.getComponentChanges());
		componentChanges.sort(Comparator.comparing(ComponentChange::getComponentId));

		assertEquals(2, componentChanges.size());
		int a = 0;
		assertChange(ComponentType.DESCRIPTION, ComponentChangeType.DELETE, componentChanges.get(a++));
		assertChange(ComponentType.DESCRIPTION, ComponentChangeType.CREATE, componentChanges.get(a++));
	}

	@Test
	public void consumeInactivationTest() throws IOException, InterruptedException {
		streamTestDataAndRetrievePersistedActivities("traceability-concept-inactivation.txt");

		final Branch branch = branchRepository.findByBranchPath("MAIN/CRSJAN19/CRSJAN19-832");
		final List<Activity> activities = activityRepository.findByHighestPromotedBranchOrderByCommitDate(branch);
		assertEquals(1, activities.size());
		final Activity activity = activities.get(0);
		assertEquals(ActivityType.CONTENT_CHANGE, activity.getActivityType());
		Set<ConceptChange> conceptChanges = activity.getConceptChanges();
		// There are 4 changes in the payload at this level but only 1 is against a concept id
		assertEquals(1, conceptChanges.size());
		ConceptChange conceptChange = conceptChanges.iterator().next();
		assertEquals("397946003", conceptChange.getConceptId().toString());
		List<ComponentChange> componentChanges = new ArrayList<>(conceptChange.getComponentChanges());
		componentChanges.sort(Comparator.comparing(ComponentChange::getComponentId));

		assertEquals(10, componentChanges.size());
		int a = 0;
		assertChange(ComponentType.DESCRIPTION, ComponentChangeType.UPDATE, componentChanges.get(a++));
		assertChange(ComponentType.DESCRIPTION, ComponentChangeType.UPDATE, componentChanges.get(a++));
		assertChange(ComponentType.RELATIONSHIP, ComponentChangeType.INACTIVATE, componentChanges.get(a++));
		assertChange(ComponentType.RELATIONSHIP, ComponentChangeType.INACTIVATE, componentChanges.get(a++));
		assertChange(ComponentType.RELATIONSHIP, ComponentChangeType.INACTIVATE, componentChanges.get(a++));
		assertChange(ComponentType.RELATIONSHIP, ComponentChangeType.INACTIVATE, componentChanges.get(a++));
		assertChange(ComponentType.DESCRIPTION, ComponentChangeType.DELETE, componentChanges.get(a++));
		assertChange(ComponentType.DESCRIPTION, ComponentChangeType.DELETE, componentChanges.get(a++));
		assertChange(ComponentType.CONCEPT, ComponentChangeType.INACTIVATE, componentChanges.get(a++));
		assertChange(ComponentType.RELATIONSHIP, ComponentChangeType.INACTIVATE, componentChanges.get(a++));
	}

	private void assertChange(ComponentType componentType, ComponentChangeType changeType, ComponentChange componentChange) {
		assertEquals(componentType, componentChange.getComponentType());
		assertEquals(changeType, componentChange.getChangeType());
	}

	@Test
	public void consumeLargeInactivationTest() throws IOException, InterruptedException {

		streamTestDataAndRetrievePersistedActivities("traceability-large-inactivation.txt");

		final Branch branch = branchRepository.findByBranchPath("MAIN/WRPSUL/WRPSUL-89");
		final Page<Activity> activities = activityRepository.findOnBranch(branch, getPageRequestMax());
		int inferredRelationshipChanges = 0;
		int statedRelationshipChanges = 0;
		Set<Long> conceptStatedChanges = new HashSet<>();
		Set<Long> conceptInferredChanges = new HashSet<>();
		for (Activity activity : activities.getContent()) {
			for (ConceptChange conceptChange : activity.getConceptChanges()) {
				for (ComponentChange componentChange : conceptChange.getComponentChanges()) {
					if (componentChange.getComponentType() == ComponentType.RELATIONSHIP) {
						if (componentChange.getComponentSubType() == ComponentSubType.INFERRED_RELATIONSHIP) {
							inferredRelationshipChanges++;
							conceptInferredChanges.add(conceptChange.getConceptId());
						} else {
							statedRelationshipChanges++;
							conceptStatedChanges.add(conceptChange.getConceptId());
						}
					}
				}
			}
		}

		assertEquals(499, statedRelationshipChanges);
		assertEquals(1167, inferredRelationshipChanges);
		assertEquals(458, conceptStatedChanges.size());
		conceptInferredChanges.removeAll(conceptStatedChanges);
		assertEquals(658, conceptInferredChanges.size());
	}

	@Test
	public void testSerialisedForm() throws IOException, InterruptedException {
		final String resource = "traceability-concept-deletion.txt";
		streamTestDataAndRetrievePersistedActivities(resource);

		final HttpURLConnection urlConnection = (HttpURLConnection) new URL("http://127.0.0.1:8085/activities").openConnection();
		assertEquals(200, urlConnection.getResponseCode());
		try (final InputStream inputStream = urlConnection.getInputStream()) {
			Assert.assertTrue(StreamUtils.copyToString(inputStream, Charset.forName("UTF-8")).contains("\"conceptId\":\"715891009\""));
		}
	}

	@Test
	public void testFindByConceptId() throws IOException, InterruptedException {
		final String resource = "traceability-example-update.txt";

		streamTestDataAndRetrievePersistedActivities(resource);

		final Page<Activity> activities = activityRepository.findByConceptId(416390003L, new PageRequest(0, 10));
		assertEquals(1, activities.getNumberOfElements());
	}

	@Test
	public void testErrorHandling() throws IOException, InterruptedException {
		streamTestDataAndRetrievePersistedActivities("traceability-invalid-content-message.txt");
		Iterable<Activity> all = activityRepository.findAll();
		assertFalse(all.iterator().hasNext());
	}

	private PageRequest getPageRequestMax() {
		return new PageRequest(0, Integer.MAX_VALUE);
	}

	private ArrayList<Activity> streamTestDataAndRetrievePersistedActivities(String resource) throws IOException, InterruptedException {
		long startingActivityCount = activityRepository.count();
		long activitiesSent = 0;

		try (final BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(resource)))) {
			String line;
			while ((line = reader.readLine()) != null) {
				sendMessage(line);
				activitiesSent++;
			}
		}

		int timeoutSeconds = 10;
		int waitedSeconds = 0;
		while (activityRepository.count() != startingActivityCount + activitiesSent && waitedSeconds < timeoutSeconds) {
			Thread.sleep(1000);
			waitedSeconds++;
		}

		final ArrayList<Activity> activities = new ArrayList<>();
		activityRepository.findAll().forEach(activities::add);
		Assert.assertNotNull(activities);
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
