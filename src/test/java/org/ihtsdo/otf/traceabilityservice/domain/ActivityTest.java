package org.ihtsdo.otf.traceabilityservice.domain;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ActivityTest {

	@Test
	void testGetBranchDepth() {
		assertEquals(1, Activity.getBranchDepth("MAIN"));
		assertEquals(2, Activity.getBranchDepth("MAIN/PROJ"));
		assertEquals(3, Activity.getBranchDepth("MAIN/PROJ/PROJ-12"));

		assertEquals(1, Activity.getBranchDepth("MAIN/SNOMEDCT-BE"));
		assertEquals(2, Activity.getBranchDepth("MAIN/SNOMEDCT-BE/BE"));
		assertEquals(3, Activity.getBranchDepth("MAIN/SNOMEDCT-BE/BE/BE-100"));

		assertEquals(1, Activity.getBranchDepth("MAIN/SNOMEDCT-ES/SNOMEDCT-AG"));
		assertEquals(2, Activity.getBranchDepth("MAIN/SNOMEDCT-ES/SNOMEDCT-AG/AG"));
		assertEquals(3, Activity.getBranchDepth("MAIN/SNOMEDCT-ES/SNOMEDCT-AG/AG/AG-100"));
	}

}
