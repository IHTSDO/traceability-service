package org.ihtsdo.otf.traceabilityservice;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
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

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

public class ApplicationIntegrationTest {

	private ConfigurableApplicationContext context;
	private ActivityRepository activityRepository;
	private BranchRepository branchRepository;

	@Before
	public void setup() throws LogLoaderException {
		// Clean out any ActiveMQ data from a previous run
		FileSystemUtils.deleteRecursively(new File("activemq-data"));
		Application.main(new String[]{});
		context = Application.getContext();
		activityRepository = context.getBean(ActivityRepository.class);
		branchRepository = context.getBean(BranchRepository.class);
	}

	@After
	public void tearDown() {
		context.close();
	}

	@Test
	public void consumeClassificationTest() throws IOException, InterruptedException {
		final String resource = "traceability-classification-save.txt";

		final ArrayList<Activity> activities = streamTestDataAndRetrievePersistedActivities(resource);

		Assert.assertEquals(1, activities.size());
		final Activity activity = activities.get(0);
		Assert.assertEquals("snowowl", activity.getUser().getUsername());
		Assert.assertEquals("MAIN/CONREQEXT/CONREQEXT-442", activity.getBranch().getBranchPath());
		Assert.assertEquals(ActivityType.CLASSIFICATION_SAVE, activity.getActivityType());
		final Set<ConceptChange> conceptChanges = activity.getConceptChanges();
		Assert.assertEquals(5, conceptChanges.size());
		final Map<Long, ConceptChange> conceptChangeMap = getConceptChangeMap(conceptChanges);
		final ConceptChange conceptChange = conceptChangeMap.get(426560005L);
		Assert.assertEquals("426560005", conceptChange.getConceptId().toString());
		final Set<ComponentChange> componentChanges = conceptChange.getComponentChanges();
		Assert.assertEquals(3, componentChanges.size());
		Assert.assertTrue(componentChanges.contains(new ComponentChange("6552672027", ComponentChangeType.CREATE, ComponentType.RELATIONSHIP, ComponentSubType.INFERRED_RELATIONSHIP)));
		Assert.assertTrue(componentChanges.contains(new ComponentChange("3207822025", ComponentChangeType.INACTIVATE, ComponentType.RELATIONSHIP, ComponentSubType.INFERRED_RELATIONSHIP)));
		Assert.assertTrue(componentChanges.contains(new ComponentChange("3198463025", ComponentChangeType.INACTIVATE, ComponentType.RELATIONSHIP, ComponentSubType.INFERRED_RELATIONSHIP)));
	}

	@Test
	public void consumeClassificationDelAndCreateTest() throws IOException, InterruptedException {
		final String resource = "traceability-classif-rel-del-create.txt";

		final ArrayList<Activity> activities = streamTestDataAndRetrievePersistedActivities(resource);

		Assert.assertEquals(1, activities.size());
		final Activity activity = activities.get(0);
		Assert.assertEquals(ActivityType.CLASSIFICATION_SAVE, activity.getActivityType());
	}

	@Test
	public void consumeClassificationRelationshipDeletionOnlyTest() throws IOException, InterruptedException {
		final String resource = "traceability-classif-rel-del-only.txt";

		final ArrayList<Activity> activities = streamTestDataAndRetrievePersistedActivities(resource);

		Assert.assertEquals(1, activities.size());
		final Activity activity = activities.get(0);
		Assert.assertEquals(ActivityType.CLASSIFICATION_SAVE, activity.getActivityType());
	}

	@Test
	public void consumeManualChangeRelationshipDeletionOnlyTest() throws IOException, InterruptedException {
		final String resource = "traceability-manual-rel-del-only.txt";

		final ArrayList<Activity> activities = streamTestDataAndRetrievePersistedActivities(resource);

		Assert.assertEquals(1, activities.size());
		final Activity activity = activities.get(0);
		Assert.assertEquals(ActivityType.CONTENT_CHANGE, activity.getActivityType());
	}

	@Test
	public void consumeConceptUpdateTest() throws IOException, InterruptedException {
		final String resource = "traceability-example-update.txt";

		final ArrayList<Activity> activities = streamTestDataAndRetrievePersistedActivities(resource);

		Assert.assertEquals(1, activities.size());
		final Activity activity = activities.get(0);
		Assert.assertEquals("snowowl", activity.getUser().getUsername());
		Assert.assertEquals("MAIN/CONREQEXT/CONREQEXT-442", activity.getBranch().getBranchPath());
		Assert.assertEquals(ActivityType.CONTENT_CHANGE, activity.getActivityType());
		final Set<ConceptChange> conceptChanges = activity.getConceptChanges();
		Assert.assertEquals(1, conceptChanges.size());
		final ConceptChange conceptChange = conceptChanges.iterator().next();
		Assert.assertEquals("416390003", conceptChange.getConceptId().toString());
		final Set<ComponentChange> componentChanges = conceptChange.getComponentChanges();
		Assert.assertEquals(3, componentChanges.size());
		Assert.assertTrue(componentChanges.contains(new ComponentChange("2546600013", ComponentChangeType.UPDATE, ComponentType.DESCRIPTION, ComponentSubType.FSN_DESCRIPTION)));
		Assert.assertTrue(componentChanges.contains(new ComponentChange("3305226012", ComponentChangeType.CREATE, ComponentType.DESCRIPTION, ComponentSubType.SYNONYM_DESCRIPTION)));
		Assert.assertTrue(componentChanges.contains(new ComponentChange("3305227015", ComponentChangeType.CREATE, ComponentType.DESCRIPTION, ComponentSubType.FSN_DESCRIPTION)));
	}

	@Test
	public void consumeConceptDeleteTest() throws IOException, InterruptedException {
		final String resource = "traceability-concept-deletion.txt";

		final ArrayList<Activity> activities = streamTestDataAndRetrievePersistedActivities(resource);

		Assert.assertEquals(1, activities.size());
		final Activity activity = activities.get(0);
		Assert.assertEquals("snowowl", activity.getUser().getUsername());
		Assert.assertEquals("MAIN/CONREQEXT/CONREQEXT-374", activity.getBranch().getBranchPath());
		Assert.assertEquals(ActivityType.CONTENT_CHANGE, activity.getActivityType());
		final Set<ConceptChange> conceptChanges = activity.getConceptChanges();
		Assert.assertEquals(1, conceptChanges.size());
		final ConceptChange conceptChange = conceptChanges.iterator().next();
		Assert.assertEquals("715891009", conceptChange.getConceptId().toString());
		final Set<ComponentChange> componentChanges = conceptChange.getComponentChanges();
		Assert.assertEquals(7, componentChanges.size());
		Assert.assertTrue(componentChanges.contains(new ComponentChange("715891009", ComponentChangeType.DELETE, ComponentType.CONCEPT, null)));
		Assert.assertTrue(componentChanges.contains(new ComponentChange("3302620017", ComponentChangeType.DELETE, ComponentType.DESCRIPTION, null)));
		Assert.assertTrue(componentChanges.contains(new ComponentChange("6546557027", ComponentChangeType.DELETE, ComponentType.RELATIONSHIP, null)));
	}

	@Test
	public void consumeBranchRebaseTest() throws IOException, InterruptedException {
		final String resource = "traceability-branch-rebase.txt";

		final ArrayList<Activity> activities = streamTestDataAndRetrievePersistedActivities(resource);

		Assert.assertEquals(1, activities.size());
		final Activity activity = activities.get(0);
		Assert.assertEquals("kkewley", activity.getUser().getUsername());
		Assert.assertEquals("MAIN/CMTFH/CMTFH-6", activity.getBranch().getBranchPath());
		Assert.assertEquals("MAIN/CMTFH", activity.getMergeSourceBranch().getBranchPath());
		Assert.assertEquals(ActivityType.REBASE, activity.getActivityType());
		final Set<ConceptChange> conceptChanges = activity.getConceptChanges();
		Assert.assertEquals(0, conceptChanges.size());
	}

	@Test
	public void consumeBranchPromoteTest() throws IOException, InterruptedException {
		final String resource = "traceability-branch-promote.txt";

		final ArrayList<Activity> activities = streamTestDataAndRetrievePersistedActivities(resource);

		Assert.assertEquals(1, activities.size());
		final Activity activity = activities.get(0);
		Assert.assertEquals("kkewley", activity.getUser().getUsername());
		Assert.assertEquals("MAIN/CMTFH", activity.getBranch().getBranchPath());
		Assert.assertEquals("MAIN/CMTFH/CMTFH-6", activity.getMergeSourceBranch().getBranchPath());
		Assert.assertEquals(ActivityType.PROMOTION, activity.getActivityType());
		final Set<ConceptChange> conceptChanges = activity.getConceptChanges();
		Assert.assertEquals(0, conceptChanges.size());
	}

	@Test
	public void consumeCreateRebasePromoteTest() throws IOException, InterruptedException {

		streamTestDataAndRetrievePersistedActivities("traceability-create-rebase.txt");

		final Branch projectBranch = branchRepository.findByBranchPath("MAIN/CMTFH");
		final List<Activity> activitiesAtProjectBeforePromotion = activityRepository.findByHighestPromotedBranchOrderByCommitDate(projectBranch);
		Assert.assertEquals(0, activitiesAtProjectBeforePromotion.size());

		streamTestDataAndRetrievePersistedActivities("traceability-branch-promote.txt");

		final List<Activity> activitiesAtProjectAfterPromotion = activityRepository.findByHighestPromotedBranchOrderByCommitDate(projectBranch);
		Assert.assertEquals(3, activitiesAtProjectAfterPromotion.size());
		final Activity activity = activitiesAtProjectAfterPromotion.get(0);
		Assert.assertEquals(ActivityType.CONTENT_CHANGE, activity.getActivityType());
		Assert.assertEquals("716755000", activity.getConceptChanges().iterator().next().getConceptId().toString());
	}

	@Test
	public void consumeLargeInactivationTest() throws IOException, InterruptedException {

		streamTestDataAndRetrievePersistedActivities("traceability-large-inactivation.txt");

		final Branch branch = branchRepository.findByBranchPath("MAIN/WRPSUL/WRPSUL-89");
		final Page<Activity> activities = activityRepository.findOnBranch(branch, getPageRequestMax());
		int inferredRelationshipChanges = 0;
		int statedRelationshipChanges = 0;
		for (Activity activity : activities.getContent()) {
			for (ConceptChange conceptChange : activity.getConceptChanges()) {
				for (ComponentChange componentChange : conceptChange.getComponentChanges()) {
					if (componentChange.getComponentType() == ComponentType.RELATIONSHIP) {
						if (componentChange.getComponentSubType() == ComponentSubType.INFERRED_RELATIONSHIP) {
							inferredRelationshipChanges++;
						} else {
							statedRelationshipChanges++;
						}
					}
				}
			}
		}

		Assert.assertEquals(1167, inferredRelationshipChanges);
		Assert.assertEquals(499, statedRelationshipChanges);
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
		Iterables.addAll(activities, activityRepository.findAll());
		Assert.assertNotNull(activities);
		return activities;
	}

	private Map<Long, ConceptChange> getConceptChangeMap(Set<ConceptChange> conceptChanges) {
		return Maps.uniqueIndex(conceptChanges, new Function<ConceptChange, Long>() {
			@Override
			public Long apply(ConceptChange conceptChange) {
				return conceptChange.getConceptId();
			}
		});
	}

	private void sendMessage(final String message) {
		MessageCreator messageCreator = new MessageCreator() {
			@Override
			public Message createMessage(Session session) throws JMSException {
				return session.createTextMessage(message);
			}
		};
		JmsTemplate jmsTemplate = context.getBean(JmsTemplate.class);
		jmsTemplate.send(Application.TRACEABILITY_QUEUE_NAME, messageCreator);
	}

}
