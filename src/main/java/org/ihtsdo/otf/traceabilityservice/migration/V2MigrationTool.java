package org.ihtsdo.otf.traceabilityservice.migration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.ihtsdo.otf.traceabilityservice.domain.Activity;
import org.ihtsdo.otf.traceabilityservice.domain.ActivityType;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;

// Tool for migrating from Traceability Service version 2 to version 3
@Service
public class V2MigrationTool {

	private static final TypeReference<V2Page<V2Activity>> PAGE_TYPE_REFERENCE = new TypeReference<>() {};

	private final ObjectMapper objectMapper;

	@Autowired
	private ActivityRepository repository;// V3 repo

	private final Logger logger = LoggerFactory.getLogger(getClass());

	public V2MigrationTool() {
		objectMapper = Jackson2ObjectMapperBuilder.json().featuresToDisable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();
	}

	public void readPage(InputStream inputStream) throws IOException {
		final V2Page<V2Activity> v2Activities = objectMapper.readValue(inputStream, PAGE_TYPE_REFERENCE);
		logger.info("Migrating v2 activity page {} of {}", v2Activities.getNumber(), v2Activities.getTotalPages());
		v2Activities.getContent().forEach(v2Activity -> {
			try {
				repository.save(new Activity(
						v2Activity.getUser().getUsername(),
						v2Activity.getBranch().getBranchPath(),
						v2Activity.getMergeSourceBranch() != null ? v2Activity.getMergeSourceBranch().getBranchPath() : null,
						v2Activity.getCommitDate(),
						ActivityType.valueOf(v2Activity.getActivityType())));
			} catch (Exception e) {
				logger.error("Failed to convert activity {} on page {}", v2Activity != null ? v2Activity.getId() : null, v2Activities.getNumber(), e);
			}
		});
	}

}
