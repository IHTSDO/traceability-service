package org.ihtsdo.otf.traceabilityservice.setup;

public class LogLoaderException extends Exception {
	public LogLoaderException(String message) {
		super(message);
	}

	public LogLoaderException(String message, Throwable cause) {
		super(message, cause);
	}
}
