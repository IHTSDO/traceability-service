package org.ihtsdo.otf.traceabilityservice.rest;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value= HttpStatus.BAD_REQUEST, reason="Bad request input")
public class BadRequestException extends RuntimeException {
}
