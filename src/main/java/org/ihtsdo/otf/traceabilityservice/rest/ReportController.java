package org.ihtsdo.otf.traceabilityservice.rest;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.ihtsdo.otf.traceabilityservice.domain.ChangeSummaryReport;
import org.ihtsdo.otf.traceabilityservice.domain.DiffReport;
import org.ihtsdo.otf.traceabilityservice.service.ArchiveDiffService;
import org.ihtsdo.otf.traceabilityservice.service.ReportService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ServerErrorException;

import java.io.IOException;
import java.io.InputStream;

@RestController("/report")
@RequestMapping(produces = MediaType.APPLICATION_JSON_VALUE)
public class ReportController {

	@Autowired
	private ReportService reportService;

	@Autowired
	private ArchiveDiffService archiveDiffService;

	@ApiOperation("Fetch change summary report on a branch since the last promotion (or versioning if a code system branch).")
	@GetMapping("/change-summary")
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

	@ApiOperation(value = "Branch change summary verses delta archive diff.",
			notes = "Report component changes that are in traceability but not in the RF2 delta archive and vice versa.")
	@PostMapping(value = "/change-summary-archive-diff", consumes = "multipart/form-data")
	public DiffReport changeSummaryArchiveDiff(
			@ApiParam(required = true)
			@RequestParam String branch,

			@ApiParam(required = true)
			@RequestParam MultipartFile rf2DeltaArchive) {

		final ChangeSummaryReport storeReport = reportService.createChangeSummaryReport(branch);
		try (final InputStream inputStream = rf2DeltaArchive.getInputStream()) {
			return archiveDiffService.diff(storeReport, inputStream);
		} catch (IOException e) {
			throw new ServerErrorException("Failed to process RF2 delta archive.", e);
		}
	}

}
