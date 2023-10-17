package org.ihtsdo.otf.traceabilityservice;

import org.ihtsdo.otf.traceabilityservice.configuration.ApplicationProperties;
import org.ihtsdo.otf.traceabilityservice.configuration.Config;
import org.ihtsdo.otf.traceabilityservice.configuration.TestActiveMQContainer;
import org.ihtsdo.otf.traceabilityservice.configuration.TestElasticsearchContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

@PropertySource("classpath:/application.properties")
@PropertySource("classpath:/application-test.properties")
@TestConfiguration
@SpringBootApplication
public class TestConfig extends Config {
	// Set to true to use local standalone Elasticsearch instance rather than Docker test container
	private static final boolean useLocalElasticsearch = false;
	private static final Logger LOGGER = LoggerFactory.getLogger(TestConfig.class);
	private static final ElasticsearchContainer elasticsearchContainer;
	private static final GenericContainer<TestActiveMQContainer> activemqContainer;

	static {
		if (useLocalElasticsearch) {
			elasticsearchContainer = null;
		} else {
			if (!DockerClientFactory.instance().isDockerAvailable()) {
				LOGGER.error("No docker client available to run integration tests.");
				LOGGER.info("Integration tests use the TestContainers framework.(https://www.testcontainers.org)");
				LOGGER.info("TestContainers framework requires docker to be installed.(https://www.testcontainers.org/supported_docker_environment)");
				LOGGER.info("You can download docker via (https://docs.docker.com/get-docker)");
				System.exit(-1);
			}
			elasticsearchContainer = new TestElasticsearchContainer();
			elasticsearchContainer.start();

			activemqContainer = new TestActiveMQContainer();
			activemqContainer.start();
		}
	}

	@Bean
	public String destinationName(@Value("${platform.name}") String platformName) {
		return platformName + "." + ApplicationProperties.TRACEABILITY_QUEUE_SUFFIX;
	}
}
