package org.ihtsdo.otf.traceabilityservice.migration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.ihtsdo.otf.traceabilityservice.AbstractTest;
import org.ihtsdo.otf.traceabilityservice.domain.Activity;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.ihtsdo.otf.traceabilityservice.rest.ActivityController;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class V2MigrationToolTest extends AbstractTest {

	private static final TypeReference<V2Page<V2Activity>> PAGE_TYPE_REFERENCE = new TypeReference<>() {};

	@Autowired
	private V2MigrationTool migrationTool;

	@Autowired
	private ActivityRepository activityRepository;

	@Autowired
	private ObjectMapper objectMapper;

	@Test
	void testMigratePage() throws IOException {
		assertEquals(0, activityRepository.count());

		final V2Page<V2Activity> v2Page = migrationTool.getObjectMapper().readValue(getClass().getResourceAsStream("version2-page.json"), PAGE_TYPE_REFERENCE);
		migrationTool.readPage(v2Page);

		assertEquals(67, activityRepository.count());
		final Page<Activity> byConceptId = activityRepository.findByConceptId(30641000087103L, PageRequest.of(0, 2, ActivityController.COMMIT_DATE_SORT));
		final List<Activity> content = byConceptId.getContent();
		assertEquals(2, content.size());
		final Activity activity = content.get(0);
		// Remove id from JSON before compare result because the value is created at runtime.
		String activityJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(activity);
		Map<String, Object> objectJsonMap = objectMapper.readValue(activityJson, new TypeReference<>() {});
		objectJsonMap.remove("id");

		assertEquals("""
                {
                  "username" : "mbraithwaite",
                  "branch" : "MAIN/CRSJAN22/CRSJAN22-404",
                  "branchDepth" : 3,
                  "highestPromotedBranch" : "MAIN/CRSJAN22/CRSJAN22-404",
                  "commitDate" : "2021-08-19T14:53:45.000+00:00",
                  "promotionDate" : "2021-08-19T14:53:45.000+00:00",
                  "activityType" : "CONTENT_CHANGE",
                  "conceptChanges" : [ {
                    "conceptId" : "30641000087103",
                    "componentChanges" : [ {
                      "componentId" : "4636190013",
                      "changeType" : "CREATE",
                      "componentType" : "DESCRIPTION",
                      "componentSubType" : "900000000000013009",
                      "effectiveTimeNull" : true
                    } ]
                  } ]
                }""", objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectJsonMap));
	}

}
