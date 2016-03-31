package org.ihtsdo.otf.traceabilityservice;

import org.ihtsdo.otf.traceabilityservice.setup.LogLoader;
import org.ihtsdo.otf.traceabilityservice.setup.LogLoaderException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jms.annotation.EnableJms;

import java.io.File;

@SpringBootApplication
@EnableJms
public class Application {

	public static final String TRACEABILITY_STREAM = "traceability-stream";

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

	protected static ConfigurableApplicationContext getContext() {
		return context;
	}
}
