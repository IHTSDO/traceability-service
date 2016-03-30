package org.ihtsdo.otf.traceabilityservice;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.ihtsdo.otf.traceabilityservice.domain.*;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.util.FileSystemUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

public class ApplicationIntegrationTest {

	private ConfigurableApplicationContext context;
	private ActivityRepository activityRepository;

	@Before
	public void setup() {
		// Clean out any ActiveMQ data from a previous run
		FileSystemUtils.deleteRecursively(new File("activemq-data"));
		Application.main(new String[]{});
		context = Application.getContext();
		activityRepository = context.getBean(ActivityRepository.class);
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
		Assert.assertEquals("MAIN/CONREQEXT/CONREQEXT-442", activity.getBranch().getBranchPath());
		Assert.assertEquals(ActivityType.CLASSIFICATION_SAVE, activity.getActivityType());
		final Set<ConceptChange> conceptChanges = activity.getConceptChanges();
		Assert.assertEquals(5, conceptChanges.size());
		final Map<Long, ConceptChange> conceptChangeMap = getConceptChangeMap(conceptChanges);
		final ConceptChange conceptChange = conceptChangeMap.get(426560005L);
		Assert.assertEquals(new Long(426560005), conceptChange.getConceptId());
		final Set<ComponentChange> componentChanges = conceptChange.getComponentChanges();
		Assert.assertEquals(3, componentChanges.size());
		Assert.assertTrue(componentChanges.contains(new ComponentChange(ComponentType.RELATIONSHIP, "6552672027", ComponentChangeType.CREATE)));
		Assert.assertTrue(componentChanges.contains(new ComponentChange(ComponentType.RELATIONSHIP, "3207822025", ComponentChangeType.INACTIVATE)));
		Assert.assertTrue(componentChanges.contains(new ComponentChange(ComponentType.RELATIONSHIP, "3198463025", ComponentChangeType.INACTIVATE)));
	}

	@Test
	public void consumeConceptUpdateTest() throws IOException, InterruptedException {
		final String resource = "traceability-example-update.txt";

		final ArrayList<Activity> activities = streamTestDataAndRetrievePersistedActivities(resource);

		Assert.assertEquals(1, activities.size());
		final Activity activity = activities.get(0);
		Assert.assertEquals("MAIN/CONREQEXT/CONREQEXT-442", activity.getBranch().getBranchPath());
		Assert.assertEquals(ActivityType.CONTENT_CHANGE, activity.getActivityType());
		final Set<ConceptChange> conceptChanges = activity.getConceptChanges();
		Assert.assertEquals(1, conceptChanges.size());
		final ConceptChange conceptChange = conceptChanges.iterator().next();
		Assert.assertEquals(new Long(416390003), conceptChange.getConceptId());
		final Set<ComponentChange> componentChanges = conceptChange.getComponentChanges();
		Assert.assertEquals(3, componentChanges.size());
		Assert.assertTrue(componentChanges.contains(new ComponentChange(ComponentType.DESCRIPTION, "2546600013", ComponentChangeType.UPDATE)));
		Assert.assertTrue(componentChanges.contains(new ComponentChange(ComponentType.DESCRIPTION, "3305226012", ComponentChangeType.CREATE)));
		Assert.assertTrue(componentChanges.contains(new ComponentChange(ComponentType.DESCRIPTION, "3305227015", ComponentChangeType.CREATE)));
	}

	@Test
	public void consumeBranchRebaseTest() throws IOException, InterruptedException {
		final String resource = "traceability-branch-rebase.txt";

		final ArrayList<Activity> activities = streamTestDataAndRetrievePersistedActivities(resource);

		Assert.assertEquals(1, activities.size());
		final Activity activity = activities.get(0);
		Assert.assertEquals("MAIN/CMTFH/CMTFH-6", activity.getBranch().getBranchPath());
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
		Assert.assertEquals("MAIN/CMTFH", activity.getBranch().getBranchPath());
		Assert.assertEquals(ActivityType.PROMOTION, activity.getActivityType());
		final Set<ConceptChange> conceptChanges = activity.getConceptChanges();
		Assert.assertEquals(0, conceptChanges.size());
	}

	private ArrayList<Activity> streamTestDataAndRetrievePersistedActivities(String resource) throws IOException, InterruptedException {
		final InputStream resourceAsStream = getClass().getResourceAsStream(resource);

		try (final BufferedReader reader = new BufferedReader(new InputStreamReader(resourceAsStream))) {
			String line;
			while ((line = reader.readLine()) != null) {
				sendMessage(line);
			}
		}

		Iterable<Activity> activitiesIterable = null;
		int timeoutSeconds = 10;
		int waitedSeconds = 0;
		while ((activitiesIterable == null || !activitiesIterable.iterator().hasNext()) && waitedSeconds < timeoutSeconds) {
			Thread.sleep(1000);
			activitiesIterable = activityRepository.findAll();
			waitedSeconds++;
		}

		final ArrayList<Activity> activities = new ArrayList<>();
		Iterables.addAll(activities, activitiesIterable);
		Assert.assertNotNull(activities);
		return activities;
	}

	private Map<Long, ConceptChange> getConceptChangeMap(Set<ConceptChange> conceptChanges) {
		return Maps.uniqueIndex(conceptChanges, new Function<ConceptChange, Long>() {
			@Nullable
			@Override
			public Long apply(@Nullable ConceptChange conceptChange) {
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
		jmsTemplate.send(Application.TRACEABILITY_STREAM, messageCreator);
	}

}
