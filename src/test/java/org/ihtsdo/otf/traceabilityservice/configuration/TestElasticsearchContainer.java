package org.ihtsdo.otf.traceabilityservice.configuration;

import org.testcontainers.elasticsearch.ElasticsearchContainer;

/**
 * Configuration for ElasticsearchContainer.
 */
public class TestElasticsearchContainer extends ElasticsearchContainer {
    public TestElasticsearchContainer() {
        super("docker.elastic.co/elasticsearch/elasticsearch:8.10.2");
        this.addFixedExposedPort(9200, 9200);
        this.addEnv("cluster.name", "integration-test-cluster");
        this.addEnv("xpack.security.enabled", "false");
    }
}
