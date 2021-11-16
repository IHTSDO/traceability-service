package org.ihtsdo.otf.traceabilityservice.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.ihtsdo.otf.traceabilityservice.migration.V2MigrationTool;
import org.ihtsdo.otf.traceabilityservice.migration.V3Point1MigrationTool;
import org.ihtsdo.otf.traceabilityservice.migration.V3Point2MigrationTool;
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
@Api(tags = "Migration", description = "Data Migration")
public class MigrationController {

	@Autowired
	private V2MigrationTool v3migrationTool;

	@Autowired
	private V3Point1MigrationTool v3Point1MigrationTool;

	@Autowired
	private V3Point2MigrationTool v3Point2MigrationTool;

	@Value("${migration.password}")
	private String migrationPassword;

	@ApiOperation("Migrate data from V2 API to this V3 API.")
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

		checkMigrationPassword(migrationPassword);
		v3migrationTool.start(v2Url, startPage, endPage);
	}

	@ApiOperation("Stop v2 to v3 migration process.")
	@PostMapping(value = "/stop", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public void stopMigration(@RequestParam String migrationPassword) {
		checkMigrationPassword(migrationPassword);
		v3migrationTool.stop();
	}

	@ApiOperation(value = "Migrate store from 3.0.x to 3.1.x.",
			notes = "This automatically fills the new field 'promotionDate' based on information already in the store.")
	@PostMapping(value = "/start-3.1", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public void startThreePointOneMigration(@RequestParam String migrationPassword) {
		checkMigrationPassword(migrationPassword);
		v3Point1MigrationTool.start();
	}

	@ApiOperation(value = "Migrate store from 3.1.x to 3.2.x.",
			notes = "This automatically updates 'promotionDate' and 'highestPromotedBranch' for rebase activities with content changes when they are promoted.")
	@PostMapping(value = "/start-3.2", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public void startThreePointTwoMigration(@RequestParam String migrationPassword) {
		checkMigrationPassword(migrationPassword);
		v3Point2MigrationTool.start();
	}


	private void checkMigrationPassword(String migrationPassword) {
		if (!this.migrationPassword.equals(migrationPassword)) {
			throw new ResponseStatusException(HttpStatus.FORBIDDEN);
		}
	}

}
