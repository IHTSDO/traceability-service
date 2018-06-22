package org.ihtsdo.otf.traceabilityservice;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import org.ihtsdo.otf.traceabilityservice.setup.LogLoader;
import org.ihtsdo.otf.traceabilityservice.setup.LogLoaderException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.jms.annotation.EnableJms;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.io.File;
import java.util.TimeZone;

import static com.google.common.base.Predicates.not;
import static springfox.documentation.builders.PathSelectors.regex;

@SpringBootApplication
@EnableJms
@EnableSwagger2
public class Application {

	public static final String TRACEABILITY_QUEUE_SUFFIX = "traceability";

	private static ConfigurableApplicationContext context;

	public static void main(String[] args) throws LogLoaderException {

		File loadLogsDir = null;
		if (args.length >= 2 && args[0].equals("--loadLogs")) {
			final String loadLogsPath = args[1];
			loadLogsDir = new File(loadLogsPath);
			if (!loadLogsDir.isDirectory()) {
				throw new IllegalArgumentException("'" + loadLogsPath + "' is not a directory.");
			}
		}

		context = SpringApplication.run(Application.class, args);

		if (loadLogsDir != null) {
			context.getBean(LogLoader.class).loadLogs(loadLogsDir);
		}
	}

	@Bean
	public ObjectMapper objectMapper() {
		final ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
		final ISO8601DateFormat df = new ISO8601DateFormat();
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
		objectMapper.setDateFormat(df);
		objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
		return objectMapper;
	}

	@Bean
	public Docket api() {
		return new Docket(DocumentationType.SWAGGER_2)
						.select()
						.apis(RequestHandlerSelectors.any())
						.paths(not(regex("/")))
						.paths(not(regex("/error")))
						.build();
	}

	protected static ConfigurableApplicationContext getContext() {
		return context;
	}
}
