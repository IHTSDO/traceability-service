package org.ihtsdo.otf.traceabilityservice.migration;

import com.fasterxml.jackson.core.type.TypeReference;
import org.ihtsdo.otf.traceabilityservice.AbstractTest;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class V2MigrationToolTest extends AbstractTest {

	private static final TypeReference<V2Page<V2Activity>> PAGE_TYPE_REFERENCE = new TypeReference<>() {};

	@Autowired
	private V2MigrationTool migrationTool;

	@Autowired
	private ActivityRepository activityRepository;

	@Test
	void testMigratePage() throws IOException {
		assertEquals(0, activityRepository.count());

		final V2Page<V2Activity> v2Page = migrationTool.getObjectMapper().readValue(getClass().getResourceAsStream("version2-page.json"), PAGE_TYPE_REFERENCE);
		migrationTool.readPage(v2Page);

		assertEquals(67, activityRepository.count());
	}

}
