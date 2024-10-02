package org.ihtsdo.otf.traceabilityservice;

import org.ihtsdo.otf.traceabilityservice.domain.Activity;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.StreamUtils;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(SpringExtension.class)
@Testcontainers
@ContextConfiguration(classes = TestConfig.class)
public abstract class AbstractTest {

	@Autowired
	protected ActivityRepository activityRepository;

	@Autowired
	protected JmsTemplate jmsTemplate;

	@Autowired
	protected String destinationName;

	@BeforeEach
	void setup() {
		jmsTemplate.setDeliveryPersistent(false);
		activityRepository.deleteAll();
	}

	protected List<Activity> sendAndReceiveActivity(String resource) throws IOException, InterruptedException {
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
		AtomicInteger found = new AtomicInteger(0);
		activityRepository.findAll(PageRequest.of(0, 100, Sort.by(Activity.Fields.COMMIT_DATE)))
				.forEach(activity -> {
					if (found.getAndIncrement() >= startingActivityCount) {
						activities.add(activity);
					}
				});
		assertNotNull(activities);
		return activities;
	}

	protected void sendMessage(final String message) {
		MessageCreator messageCreator = session -> session.createTextMessage(message);
		jmsTemplate.send(destinationName, messageCreator);
	}

}
