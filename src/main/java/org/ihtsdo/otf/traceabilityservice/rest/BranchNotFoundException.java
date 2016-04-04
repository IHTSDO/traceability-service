package org.ihtsdo.otf.traceabilityservice.rest;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value= HttpStatus.NOT_FOUND, reason="No such Branch")
public class BranchNotFoundException extends RuntimeException {
}
