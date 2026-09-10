package org.ihtsdo.otf.traceabilityservice.configuration.elasticsearch;

import io.github.acm19.aws.interceptor.http.AwsRequestSigningApacheV5Interceptor;
import org.apache.hc.core5.http.HttpRequestInterceptor;
import org.apache.hc.core5.util.Timeout;
import org.ihtsdo.otf.traceabilityservice.configuration.ApplicationProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchConfiguration;
import org.springframework.data.elasticsearch.client.elc.rest5_client.Rest5Clients;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchCustomConversions;
import org.springframework.data.elasticsearch.support.HttpHeaders;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;

import java.util.Arrays;

@Configuration
public class ElasticsearchConfig extends ElasticsearchConfiguration {
	private final ApplicationProperties applicationProperties;
	private final Logger logger = LoggerFactory.getLogger(getClass());

	public ElasticsearchConfig(ApplicationProperties applicationProperties) {
		this.applicationProperties = applicationProperties;
	}

	@Override
	public ClientConfiguration clientConfiguration() {
		HttpHeaders apiKeyHeaders = new HttpHeaders();
		if (applicationProperties.hasElasticsearchApiKey()) {
			logger.info("Using API key authentication.");
			apiKeyHeaders.add("Authorization", "ApiKey " + applicationProperties.getElasticsearchApiKey());
		}

		ClientConfiguration.TerminalClientConfigurationBuilder builder;
		if (useHttps(applicationProperties.getElasticsearchUrls())) {
			builder = ClientConfiguration.builder()
					.connectedTo(applicationProperties.getElasticsearchHosts())
					.usingSsl()
					.withDefaultHeaders(apiKeyHeaders);
		} else {
			builder = ClientConfiguration.builder()
					.connectedTo(applicationProperties.getElasticsearchHosts())
					.withDefaultHeaders(apiKeyHeaders);
		}

		if (applicationProperties.hasElasticsearchCredentials()) {
			builder = builder.withBasicAuth(applicationProperties.getElasticsearchUsername(),
					applicationProperties.getElasticsearchPassword());
		}

		return builder
				.withClientConfigurer(configureRequestConfig())
				.withClientConfigurer(configureHttpClient())
				.build();
	}

	private boolean useHttps(String[] urls) {
		for (String url : urls) {
			if (url.startsWith("https://")) {
				return true;
			}
		}
		return false;
	}

	private Rest5Clients.ElasticsearchRequestConfigCallback configureRequestConfig() {
		// Disable lease handling for the connection pool! See https://github.com/elastic/elasticsearch/issues/24069
		return Rest5Clients.ElasticsearchRequestConfigCallback.from(requestConfigBuilder ->
				requestConfigBuilder.setConnectionRequestTimeout(Timeout.DISABLED));
	}

	private Rest5Clients.ElasticsearchHttpClientConfigurationCallback configureHttpClient() {
		return Rest5Clients.ElasticsearchHttpClientConfigurationCallback.from(httpClientBuilder -> {
			if (applicationProperties.isAwsRequestSigning()) {
				httpClientBuilder.addRequestInterceptorFirst(awsInterceptor());
			}
			return httpClientBuilder;
		});
	}

	private HttpRequestInterceptor awsInterceptor() {
		return new AwsRequestSigningApacheV5Interceptor(
				"es",
				Aws4Signer.create(),
				DefaultCredentialsProvider.create(),
				DefaultAwsRegionProviderChain.builder().build().getRegion()
		);
	}

	@Bean
	public ElasticsearchCustomConversions elasticsearchCustomConversions() {
		return new ElasticsearchCustomConversions(
				Arrays.asList(new DateToLongConverter(), new LongToDateConverter()));
	}

}
