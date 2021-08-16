package org.ihtsdo.otf.traceabilityservice.configuration;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import org.ihtsdo.otf.traceabilityservice.configuration.elasticsearch.ElasticsearchConfig;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchDataAutoConfiguration;
import org.springframework.boot.autoconfigure.jms.DefaultJmsListenerContainerFactoryConfigurer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.aws.autoconfigure.context.ContextStackAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.client.RestClients;
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;

import javax.annotation.PostConstruct;
import javax.jms.ConnectionFactory;
import java.util.TimeZone;

@SpringBootApplication(
		exclude = {
				ElasticsearchDataAutoConfiguration.class,
				RestClients.ElasticsearchRestClient.class,
				ContextStackAutoConfiguration.class,
		}
)
@EnableElasticsearchRepositories(
		basePackages = {
				"org.ihtsdo.otf.traceabilityservice.repository"
		})
@EnableConfigurationProperties
@PropertySource(value = "classpath:application.properties", encoding = "UTF-8")
public abstract class Config extends ElasticsearchConfig {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	private ActivityRepository activityRepository;

	@PostConstruct
	public void checkElasticsearchConnection() {
		try {
			activityRepository.findAll(PageRequest.of(0, 1));
		} catch (DataAccessResourceFailureException e) {
			throw new IllegalStateException("Failed to connect to Elasticsearch.", e);
		}
	}

	@Bean
	public DefaultJmsListenerContainerFactory jmsListenerContainerFactory(ConnectionFactory connectionFactory, DefaultJmsListenerContainerFactoryConfigurer configurer) {
		DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
		factory.setErrorHandler(t -> logger.error("Failed to consume message.", t));
		configurer.configure(factory, connectionFactory);
		return factory;
	}

	@Bean
	public ObjectMapper objectMapper() {
		final ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
		final StdDateFormat df = new StdDateFormat();
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
		objectMapper.setDateFormat(df);
		objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
		return objectMapper;
	}

}
