package org.ihtsdo.otf.traceabilityservice.configuration;

import org.testcontainers.containers.GenericContainer;

/**
 * Configuration for ActiveMQContainer.
 */
public class TestActiveMQContainer extends GenericContainer<TestActiveMQContainer> {
    public TestActiveMQContainer() {
        super("symptoma/activemq:latest");
        this.addFixedExposedPort(61616, 61616);
    }
}
