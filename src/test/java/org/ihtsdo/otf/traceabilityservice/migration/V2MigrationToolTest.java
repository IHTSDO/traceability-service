package org.ihtsdo.otf.traceabilityservice.migration;

import org.ihtsdo.otf.traceabilityservice.AbstractTest;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class V2MigrationToolTest extends AbstractTest {

	@Autowired
	private V2MigrationTool migrationTool;

	@Autowired
	private ActivityRepository activityRepository;

	@Test
	void testMigratePage() throws IOException {
		assertEquals(0, activityRepository.count());

		migrationTool.readPage(getClass().getResourceAsStream("version2-page.json"));

		assertEquals(67, activityRepository.count());
	}

}
