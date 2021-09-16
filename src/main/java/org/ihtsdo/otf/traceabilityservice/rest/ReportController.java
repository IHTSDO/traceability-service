package org.ihtsdo.otf.traceabilityservice.rest;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.ihtsdo.otf.traceabilityservice.domain.ChangeSummaryReport;
import org.ihtsdo.otf.traceabilityservice.service.ReportService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController("/report")
@RequestMapping(produces = MediaType.APPLICATION_JSON_VALUE)
public class ReportController {

	@Autowired
	private ReportService reportService;

	@ApiOperation("Fetch change summary report on a branch since the last promotio (or versioning if a code system branch).")
	@PostMapping("/change-summary")
	public ChangeSummaryReport createChangeSummaryReport(
			@ApiParam(required = true)
			@RequestParam String branch,

			@ApiParam(defaultValue = "true")
			@RequestParam(defaultValue = "true") boolean includeMadeOnThisBranch,

			@ApiParam(defaultValue = "true")
			@RequestParam(defaultValue = "true") boolean includePromotedToThisBranch,

			@ApiParam(defaultValue = "true")
			@RequestParam(defaultValue = "true") boolean includeRebasedToThisBranch) {

		return reportService.createChangeSummaryReport(branch, includeMadeOnThisBranch, includePromotedToThisBranch, includeRebasedToThisBranch);
	}

}
