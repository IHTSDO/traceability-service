package org.ihtsdo.otf.traceabilityservice.rest;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.ihtsdo.otf.traceabilityservice.domain.ChangeSummaryReport;
import org.ihtsdo.otf.traceabilityservice.service.PatchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Tag(name = "Patch")
@RequestMapping(path = "/patch", produces = MediaType.APPLICATION_JSON_VALUE)
public class PatchController {

	@Autowired
	private PatchService patchService;

	@Operation(summary = "Patch component traceability history to workaround failing assertions.",
			description = """
                    This should only be required for one authoring cycle after upgrading to version 3.x.\s
                    When components are reported as missing from the traceability store add the ids to componentsWithoutEffectiveTime.\s
                    When components are reported as missing from the RF2 delta add the ids to componentsWithEffectiveTime.\s""")
	@PostMapping
	public ChangeSummaryReport patchHistory(
			@Parameter(required = true)
			@RequestBody PatchRequest patchRequest) {

		return patchService.patchHistory(patchRequest.getBranch(), patchRequest.getComponentsWithEffectiveTime(), patchRequest.getComponentsWithoutEffectiveTime());
	}

}
