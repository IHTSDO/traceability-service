package org.ihtsdo.otf.traceabilityservice.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.ihtsdo.otf.traceabilityservice.migration.V2MigrationTool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController("/migration")
@Api(tags = "Migration", description = "V2 Data Migration")
public class MigrationController {

	@Autowired
	private V2MigrationTool migrationTool;

	@Value("${migration.password}")
	private String migrationPassword;

	@ApiOperation("Migrate data from V2 API to this V2 API.")
	@PostMapping(value = "/start", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public void startMigration(
			@RequestParam
			String v2Url,
			@RequestParam(required = false, defaultValue = "0")
			int startPage,
			@RequestParam(required = false)
			Integer endPage,
			@RequestParam
			String migrationPassword) {

		if (!this.migrationPassword.equals(migrationPassword)) {
			throw new ResponseStatusException(HttpStatus.FORBIDDEN);
		}

		migrationTool.start(v2Url, startPage, endPage);
	}

	@ApiOperation("Stop migration process.")
	@PostMapping(value = "/stop", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public void stopMigration() {
		migrationTool.stop();
	}

}
