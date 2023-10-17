package org.ihtsdo.otf.traceabilityservice.configuration.elasticsearch;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import org.ihtsdo.otf.traceabilityservice.configuration.aws.AWSRequestSigningApacheInterceptor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchClients;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchConfiguration;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchCustomConversions;

import java.time.Duration;
import java.util.Arrays;

public class ElasticsearchConfig extends ElasticsearchConfiguration {
	@Value("${traceability.aws.request-signing.enabled}")
	private Boolean awsRequestSigning;

	@Override
	public ClientConfiguration clientConfiguration() {
		return ClientConfiguration
				.builder()
				.connectedTo("http://localhost:9200")
				.withConnectTimeout(Duration.ofSeconds(5))
				.withSocketTimeout(Duration.ofSeconds(3))
				.withClientConfigurer(
						ElasticsearchClients.ElasticsearchRestClientConfigurationCallback.from(clientBuilder -> {
							clientBuilder.setRequestConfigCallback(builder -> {
								//Disable lease handling for the connection pool! See https://github.com/elastic/elasticsearch/issues/24069
								builder.setConnectionRequestTimeout(0);
								return builder;
							});

							if (awsRequestSigning) {
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
