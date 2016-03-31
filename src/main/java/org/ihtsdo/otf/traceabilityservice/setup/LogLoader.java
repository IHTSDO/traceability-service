package org.ihtsdo.otf.traceabilityservice.setup;

import org.ihtsdo.otf.traceabilityservice.jms.TraceabilityStreamConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
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

	public void loadLogs(File loadLogsDir) throws LogLoaderException {
		final List<String> partiallyLoaded = new ArrayList<>();
		final List<String> completelyLoaded = new ArrayList<>();
		final File[] files = loadLogsDir.listFiles();
		final List<File> filesToLoad = sortFiles(files);
		for (File file : filesToLoad) {
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
					completelyLoaded.add(fileName);
				} catch (IOException e) {
					logger.error("Failed to load log file '{}'.", fileName, e);
					partiallyLoaded.add(fileName);
				} catch (NullPointerException e) {
					throw new LogLoaderException(String.format("Failed to load all of log file '%s', problem with line %s .", fileName, lineNum), e);
				}
			}
		}
		if (partiallyLoaded.isEmpty()) {
			logger.info("All log files were loaded successfully. Completely loaded: {}", completelyLoaded);
		} else {
			logger.error("Some log files failed to load. Partially loaded: {}. Completely loaded: {}", partiallyLoaded, completelyLoaded);
		}
	}

	private List<File> sortFiles(File[] files) {
		final List<File> filesToLoad = new ArrayList<>();
		Collections.addAll(filesToLoad, files);
		if (filesToLoad.size() > 1) {
			final File file = filesToLoad.get(0);
			if (file.getName().equals("snomed-traceability.log")) {
				// move to end
				filesToLoad.remove(file);
				filesToLoad.add(file);
			}
		}
		return filesToLoad;
	}

}
