package org.ihtsdo.otf.traceabilityservice.rest;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
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
@Tag(name = "Report", description = "Change summary report")
@RequestMapping(produces = MediaType.APPLICATION_JSON_VALUE)
public class ReportController {

	@Autowired
	private ReportService reportService;

	@Autowired
	private ArchiveDiffService archiveDiffService;

	@Operation(summary = "Fetch change summary report on a branch since the last promotion (or versioning if a code system branch).",
		 description = "When contentBaseTimestamp is not specified, the last promotion date will be used if present. Otherwise it will use epoch date.")
	@GetMapping("/change-summary")
	public ChangeSummaryReport createChangeSummaryReport(
			@Parameter(required = true)
			@RequestParam String branch,

			@RequestParam(required = false) Long contentBaseTimestamp,

			@RequestParam(required = false) Long contentHeadTimestamp,

			@RequestParam(defaultValue = "true") boolean includeMadeOnThisBranch,

			@RequestParam(defaultValue = "true") boolean includePromotedToThisBranch,

			@RequestParam(defaultValue = "true") boolean includeRebasedToThisBranch) {

		if (contentBaseTimestamp != null && contentHeadTimestamp != null && contentBaseTimestamp > contentHeadTimestamp) {
			throw new IllegalArgumentException(String.format("contentBaseTimestamp %d can't be later than contentHeadTimestamp %d", contentBaseTimestamp, contentHeadTimestamp));
		}
		return reportService.createChangeSummaryReport(branch, contentBaseTimestamp, contentHeadTimestamp, includeMadeOnThisBranch, includePromotedToThisBranch, includeRebasedToThisBranch);
	}

	@Operation(summary = "Branch change summary verses delta archive diff.",
			description = "Report component changes that are in traceability but not in the RF2 delta archive and vice versa.")
	@PostMapping(value = "/change-summary-archive-diff", consumes = "multipart/form-data")
	public DiffReport changeSummaryArchiveDiff(
			@Parameter(required = true)
			@RequestParam String branch,

			@Parameter(required = true)
			@RequestParam MultipartFile rf2DeltaArchive) {

		final ChangeSummaryReport storeReport = reportService.createChangeSummaryReport(branch);
		try (final InputStream inputStream = rf2DeltaArchive.getInputStream()) {
			return archiveDiffService.diff(storeReport, inputStream);
		} catch (IOException e) {
			throw new ServerErrorException("Failed to process RF2 delta archive.", e);
		}
	}

}
