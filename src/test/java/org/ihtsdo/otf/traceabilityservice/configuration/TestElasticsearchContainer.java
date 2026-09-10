package org.ihtsdo.otf.traceabilityservice.configuration;

import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * Configuration for ElasticsearchContainer.
 */
public class TestElasticsearchContainer extends ElasticsearchContainer {

	private static final DockerImageName IMAGE =
			DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:9.5.2");

	public TestElasticsearchContainer() {
		super(IMAGE);
		this.addFixedExposedPort(9200, 9200);
		this.addEnv("cluster.name", "integration-test-cluster");
		this.addEnv("discovery.type", "single-node");
		this.addEnv("xpack.security.enabled", "false");
		this.addEnv("xpack.security.http.ssl.enabled", "false");
		this.addEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m");
		this.withStartupTimeout(Duration.ofMinutes(3));
	}
}
