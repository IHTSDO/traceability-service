package org.ihtsdo.otf.traceabilityservice.configuration;

import org.apache.http.HttpHost;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class ApplicationProperties {
    public static final String TRACEABILITY_QUEUE_SUFFIX = "traceability";

    @Value("${elasticsearch.urls}")
    private String[] elasticsearchUrls;

    @Value("${elasticsearch.username}")
    private String elasticsearchUsername;

    @Value("${elasticsearch.password}")
    private String elasticsearchPassword;

    @Value("${elasticsearch.api-key}")
    private String elasticsearchApiKey;

    @Value("${traceability.aws.request-signing.enabled}")
    private String awsRequestSigning;

    @Value("${elasticsearch.index.prefix}")
    private String indexPrefix;

    @Value("${elasticsearch.index.app.prefix}")
    private String indexApplicationPrefix;

    public String[] getElasticsearchUrls() {
        return elasticsearchUrls;
    }

    public void setElasticsearchUrls(String[] elasticsearchUrls) {
        this.elasticsearchUrls = elasticsearchUrls;
    }

    public String[] getElasticsearchHosts() {
        List<String> hosts = new ArrayList<>();
        for (String url : elasticsearchUrls) {
            hosts.add(HttpHost.create(url).toHostString());
        }
        return hosts.toArray(new String[]{});
    }

    public String getElasticsearchUsername() {
        return elasticsearchUsername;
    }

    public void setElasticsearchUsername(String elasticsearchUsername) {
        this.elasticsearchUsername = elasticsearchUsername;
    }

    public String getElasticsearchPassword() {
        return elasticsearchPassword;
    }

    public void setElasticsearchPassword(String elasticsearchPassword) {
        this.elasticsearchPassword = elasticsearchPassword;
    }

    public String getElasticsearchApiKey() {
        return elasticsearchApiKey;
    }

    public void setElasticsearchApiKey(String elasticsearchApiKey) {
        this.elasticsearchApiKey = elasticsearchApiKey;
    }

    public String getAwsRequestSigning() {
        return awsRequestSigning;
    }

    public void setAwsRequestSigning(String awsRequestSigning) {
        this.awsRequestSigning = awsRequestSigning;
    }

    public boolean isAwsRequestSigning() {
        if (awsRequestSigning == null) {
            return false;
        }

        return Boolean.parseBoolean(awsRequestSigning);
    }

    public String getIndexPrefix() {
        return indexPrefix;
    }

    public void setIndexPrefix(String indexPrefix) {
        this.indexPrefix = indexPrefix;
    }

    public String getIndexApplicationPrefix() {
        return indexApplicationPrefix;
    }

    public void setIndexApplicationPrefix(String indexApplicationPrefix) {
        this.indexApplicationPrefix = indexApplicationPrefix;
    }

    public boolean hasElasticsearchCredentials() {
        return elasticsearchUsername != null && !elasticsearchUsername.isEmpty() && elasticsearchPassword != null && !elasticsearchPassword.isEmpty();
    }

    public boolean hasElasticsearchApiKey() {
        return elasticsearchApiKey != null && !elasticsearchApiKey.isEmpty();
    }
}
