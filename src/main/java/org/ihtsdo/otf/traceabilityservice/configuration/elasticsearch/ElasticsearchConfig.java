package org.ihtsdo.otf.traceabilityservice.configuration.elasticsearch;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.ihtsdo.otf.traceabilityservice.configuration.ApplicationProperties;
import org.ihtsdo.otf.traceabilityservice.configuration.aws.AWSRequestSigningApacheInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchClients;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchConfiguration;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchCustomConversions;
import org.springframework.data.elasticsearch.support.HttpHeaders;

import java.time.Duration;
import java.util.Arrays;

@Configuration
public class ElasticsearchConfig extends ElasticsearchConfiguration {
	private final ApplicationProperties applicationProperties;

	public ElasticsearchConfig(ApplicationProperties applicationProperties) {
		this.applicationProperties = applicationProperties;
	}

	@Override
	public ClientConfiguration clientConfiguration() {
		return ClientConfiguration
				.builder()
				.connectedTo(applicationProperties.getElasticsearchHosts())
				.withConnectTimeout(Duration.ofSeconds(5))
				.withSocketTimeout(Duration.ofSeconds(3))
				.withHeaders(() -> {
					HttpHeaders headers = new HttpHeaders();
					if (applicationProperties.hasElasticsearchApiKey()) {
						headers.add(HttpHeaders.AUTHORIZATION, "ApiKey " + applicationProperties.getElasticsearchApiKey());
					}

					return headers;
				})
				.withClientConfigurer(
						ElasticsearchClients.ElasticsearchRestClientConfigurationCallback.from(clientBuilder -> {
							clientBuilder.setRequestConfigCallback(builder -> {
								//Disable lease handling for the connection pool! See https://github.com/elastic/elasticsearch/issues/24069
								builder.setConnectionRequestTimeout(0);
								return builder;
							});

							if (applicationProperties.hasElasticsearchCredentials()) {
								final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
								credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(applicationProperties.getElasticsearchUsername(), applicationProperties.getElasticsearchPassword()));
								clientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
							}

							if (applicationProperties.isAwsRequestSigning()) {
								clientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.addInterceptorLast(awsInterceptor("es")));
							}

							return clientBuilder;
						})
				)
				.build();
	}

	private AWSRequestSigningApacheInterceptor awsInterceptor(String serviceName) {
		AWS4Signer signer = new AWS4Signer();
		DefaultAwsRegionProviderChain regionProviderChain = new DefaultAwsRegionProviderChain();
		DefaultAWSCredentialsProviderChain credentialsProvider = new DefaultAWSCredentialsProviderChain();
		signer.setServiceName(serviceName);
		signer.setRegionName(regionProviderChain.getRegion());

		return new AWSRequestSigningApacheInterceptor(serviceName, signer, credentialsProvider);
	}

	@Bean
	public ElasticsearchCustomConversions elasticsearchCustomConversions() {
		return new ElasticsearchCustomConversions(
				Arrays.asList(new DateToLongConverter(), new LongToDateConverter()));
	}

}
