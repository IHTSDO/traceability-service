package org.ihtsdo.otf.traceabilityservice.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Set;

import org.ihtsdo.otf.traceabilityservice.AbstractTest;
import org.junit.jupiter.api.Test;

public class BranchUtilsTest extends AbstractTest {

	@Test
	void testGetAncestorBranches() throws IOException, InterruptedException {
		String branchPath = "MAIN/foo/bar/car";
		Set<String> ancestorBranches = BranchUtils.getAncestorBranches(branchPath);
		
		assertEquals(3, ancestorBranches.size());
		assertTrue(ancestorBranches.contains("MAIN"));
		assertTrue(ancestorBranches.contains("MAIN/foo"));
		assertTrue(ancestorBranches.contains("MAIN/foo/bar"));
	}
}
