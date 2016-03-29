package org.ihtsdo.otf.traceabilityservice;

import com.google.common.collect.Iterables;
import org.ihtsdo.otf.traceabilityservice.domain.*;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.util.FileSystemUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Set;
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

	@Test
	public void sendMessageTest() throws IOException, InterruptedException {
		final String resource = "traceability-example-update.txt";
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
		Assert.assertEquals(1, activities.size());
		final Activity activity = activities.get(0);
		Assert.assertEquals("MAIN/CONREQEXT/CONREQEXT-442", activity.getBranch().getBranchPath());
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

	private void sendMessage(final String message) {
		MessageCreator messageCreator = new MessageCreator() {
			@Override
			public Message createMessage(Session session) throws JMSException {
				return session.createTextMessage(message);
			}
		};
		JmsTemplate jmsTemplate = context.getBean(JmsTemplate.class);
		jmsTemplate.send("traceability-stream", messageCreator);
	}

}