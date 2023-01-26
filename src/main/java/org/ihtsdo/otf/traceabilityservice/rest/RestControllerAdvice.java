package org.ihtsdo.otf.traceabilityservice.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.util.HashMap;
import java.util.Map;

@ControllerAdvice
public class RestControllerAdvice {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	@ExceptionHandler({
			IllegalArgumentException.class,
			IllegalStateException.class,
			HttpRequestMethodNotSupportedException.class,
			HttpMediaTypeNotSupportedException.class,
			MethodArgumentNotValidException.class,
			MethodArgumentTypeMismatchException.class,
			MissingServletRequestParameterException.class,
			HttpMessageNotReadableException.class
	})
	@ResponseStatus(HttpStatus.BAD_REQUEST)
	@ResponseBody
	public Map<String, Object> handleIllegalArgumentException(Exception exception) {
		HashMap<String, Object> result = new HashMap<>();
		result.put("error", HttpStatus.BAD_REQUEST);
		result.put("message", exception.getMessage());
		if (exception.getCause() != null) {
			result.put("causeMessage", exception.getCause().getMessage());
		}
		logger.info("bad request {}", exception.getMessage());
		logger.debug("bad request {}", exception.getMessage(), exception);
		return result;
	}
	@ExceptionHandler(Exception.class)
	@ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
	@ResponseBody
	public Map<String,Object> handleException(Exception exception) {
		logger.error(exception.getMessage(), exception);
		HashMap<String, Object> result = new HashMap<>();
		result.put("error", HttpStatus.INTERNAL_SERVER_ERROR);
		result.put("message", exception.getMessage());
		return result;
	}
}
