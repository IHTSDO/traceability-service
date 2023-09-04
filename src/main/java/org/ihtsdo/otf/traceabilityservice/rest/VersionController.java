package org.ihtsdo.otf.traceabilityservice.rest;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.info.BuildProperties;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@RestController
@Tag(name = "Version", description = "Build Version")
public class VersionController {

	@Autowired(required = false)
	BuildProperties buildProperties;

	@Operation(summary = "Software build version and timestamp.")
	@GetMapping(value = "/version", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public BuildVersion getBuildInformation() {
		if (buildProperties == null) {
			return new BuildVersion("dev", "");
		}
		return new BuildVersion(buildProperties.getVersion(), buildProperties.getTime().toString());
	}

	public static final class BuildVersion {

		private final String version;
		private final String time;

		BuildVersion(String version, String time) {
			this.version = version;
			this.time = time;
		}

		public String getVersion() {
			return version;
		}

		public String getTime() {
			return time;
		}
	}

}
