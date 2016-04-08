package org.ihtsdo.otf.traceabilityservice.rest;

import org.springframework.web.bind.annotation.PathVariable;

public class ControllerHelper {

	public static long parseLong(@PathVariable String activityId) {
		final long id;
		try {
			id = Long.parseLong(activityId);
		} catch (NumberFormatException e) {
			throw new BadRequestException();
		}
		return id;
	}

}
