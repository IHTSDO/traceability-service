package org.ihtsdo.otf.traceabilityservice.rest;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.ihtsdo.otf.traceabilityservice.domain.ChangeSummaryReport;
import org.ihtsdo.otf.traceabilityservice.service.PatchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController("/patch")
@RequestMapping(produces = MediaType.APPLICATION_JSON_VALUE)
public class PatchController {

	@Autowired
	private PatchService patchService;

	@ApiOperation(value = "Patch component traceability history to workaround failing assertions.",
			notes = """
                    This should only be required for one authoring cycle after upgrading to version 3.x.\s
                    When components are reported as missing from the traceability store add the ids to componentsWithoutEffectiveTime.\s
                    When components are reported as missing from the RF2 delta add the ids to componentsWithEffectiveTime.\s""")
	@PostMapping
	public ChangeSummaryReport patchHistory(
			@ApiParam(required = true)
			@RequestBody PatchRequest patchRequest) {

		return patchService.patchHistory(patchRequest.getBranch(), patchRequest.getComponentsWithEffectiveTime(), patchRequest.getComponentsWithoutEffectiveTime());
	}

}
