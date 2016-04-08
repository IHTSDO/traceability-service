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

import java.io.File;
import java.util.TimeZone;

@SpringBootApplication
@EnableJms
public class Application {

	public static final String TRACEABILITY_QUEUE_NAME = "traceability";

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

	protected static ConfigurableApplicationContext getContext() {
		return context;
	}
}
