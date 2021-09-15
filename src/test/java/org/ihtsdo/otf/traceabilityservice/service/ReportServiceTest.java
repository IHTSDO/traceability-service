package org.ihtsdo.otf.traceabilityservice.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Deque;

import static org.junit.jupiter.api.Assertions.*;

class ReportServiceTest {

	private final ReportService reportService = new ReportService();

	@Test
	void testCreateAncestorDeque() {
		final Deque<String> ancestors = reportService.createAncestorDeque("MAIN/PROJECTA/PROJECTA-1");
		assertEquals(2, ancestors.size());
		assertEquals("MAIN/PROJECTA", ancestors.pop());
		assertEquals("MAIN", ancestors.pop());
	}

	@Test
	void testIsCodeSystemBranch() {
		assertTrue(reportService.isCodeSystemBranch("MAIN"));
		assertFalse(reportService.isCodeSystemBranch("MAIN/PROJECT"));
		assertFalse(reportService.isCodeSystemBranch("MAIN/PROJECT/PROJECT-123"));
		assertFalse(reportService.isCodeSystemBranch("MAIN/2021-09-30"));
		assertTrue(reportService.isCodeSystemBranch("MAIN/SNOMEDCT-BE"));
		assertFalse(reportService.isCodeSystemBranch("MAIN/SNOMEDCT-BE/BE"));
		assertFalse(reportService.isCodeSystemBranch("MAIN/SNOMEDCT-BE/BE/BE-123"));
		assertTrue(reportService.isCodeSystemBranch("MAIN/SNOMEDCT-ES"));
		assertTrue(reportService.isCodeSystemBranch("MAIN/SNOMEDCT-ES/SNOMEDCT-AR"));
		assertFalse(reportService.isCodeSystemBranch("MAIN/SNOMEDCT-ES/SNOMEDCT-AR/AR"));
		assertFalse(reportService.isCodeSystemBranch("MAIN/SNOMEDCT-ES/SNOMEDCT-AR/AR/AR-123"));
	}

}