package org.ihtsdo.otf.traceabilityservice.configuration.elasticsearch;

import io.github.acm19.aws.interceptor.http.AwsRequestSigningApacheInterceptor;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.ihtsdo.otf.traceabilityservice.configuration.ApplicationProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchClients;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchConfiguration;
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

		if (useHttps(applicationProperties.getElasticsearchUrls())) {
			return ClientConfiguration.builder()
					.connectedTo(applicationProperties.getElasticsearchHosts())
					.usingSsl()
					.withDefaultHeaders(apiKeyHeaders)
					.withClientConfigurer(configureHttpClient()).build();
		} else {
			return ClientConfiguration.builder()
					.connectedTo(applicationProperties.getElasticsearchHosts())
					.withDefaultHeaders(apiKeyHeaders)
					.withClientConfigurer(configureHttpClient()).build();
		}
	}


	private boolean useHttps(String[] urls) {
		for (String url : urls) {
			if (url.startsWith("https://")) {
				return true;
			}
		}
		return false;
	}

	private ElasticsearchClients.ElasticsearchRestClientConfigurationCallback configureHttpClient() {
		return ElasticsearchClients.ElasticsearchRestClientConfigurationCallback.from(clientBuilder -> {
			clientBuilder.setRequestConfigCallback(builder -> {
				builder.setConnectionRequestTimeout(0);//Disable lease handling for the connection pool! See https://github.com/elastic/elasticsearch/issues/24069
				return builder;
			});
			final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
			if (applicationProperties.hasElasticsearchCredentials()) {
				credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(applicationProperties.getElasticsearchUsername(), applicationProperties.getElasticsearchPassword()));
			}
			clientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
				httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
				if (applicationProperties.isAwsRequestSigning()) {
					httpClientBuilder.addInterceptorFirst(awsInterceptor());
				}
				return httpClientBuilder;
			});
			return clientBuilder;
		});
	}

	private HttpRequestInterceptor awsInterceptor() {
		return new AwsRequestSigningApacheInterceptor(
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
