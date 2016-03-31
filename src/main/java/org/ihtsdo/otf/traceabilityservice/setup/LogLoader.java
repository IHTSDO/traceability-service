package org.ihtsdo.otf.traceabilityservice.setup;

import org.ihtsdo.otf.traceabilityservice.jms.TraceabilityStreamConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * This can be used to prime the database using the traceability log files
 */
@Service
public class LogLoader {

	@Autowired
	private TraceabilityStreamConsumer traceabilityStreamConsumer;

	private final Logger logger = LoggerFactory.getLogger(getClass());

	public void loadLogs(File loadLogsDir) {
		final List<String> failedFiles = new ArrayList<>();
		for (File file : loadLogsDir.listFiles()) {
			final String fileName = file.getName();
			if (file.isFile() && fileName.startsWith("snomed-traceability")) {
				logger.info("Loading {}", fileName);
				long lineNum = -1;
				try {
					final InputStream in;
					if (fileName.endsWith(".gz")) {
						in = new GZIPInputStream(new FileInputStream(file));
					} else {
						in = new FileInputStream(file);
					}
					try (final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in))) {
						String line;
						lineNum = 0;
						while ((line = bufferedReader.readLine()) != null) {
							lineNum++;
							final int beginIndex = line.indexOf("{");
							if (beginIndex != -1) {
								traceabilityStreamConsumer.receiveMessage(line.substring(beginIndex));
							} else {
								logger.warn("Line {} of {} does not contain '{' character, we are expecting JSON... skipped line.", lineNum, fileName);
							}
						}
					}
				} catch (IOException e) {
					logger.error("Failed to load log file '{}'.", fileName, e);
					failedFiles.add(fileName);
				} catch (NullPointerException e) {
					logger.error("Failed to load all of log file '{}', problem with line{} .", fileName, lineNum, e);
					failedFiles.add(fileName);
				}
			}
		}
		if (failedFiles.isEmpty()) {
			logger.info("All log files were loaded successfully.");
		} else {
			logger.error("Some log files failed to load: {}", failedFiles);
		}
	}

}
