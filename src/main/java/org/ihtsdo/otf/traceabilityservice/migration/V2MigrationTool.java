package org.ihtsdo.otf.traceabilityservice.migration;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.ihtsdo.otf.traceabilityservice.domain.Activity;
import org.ihtsdo.otf.traceabilityservice.domain.ActivityType;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

// Tool for migrating from Traceability Service version 2 to version 3
@Service
public class V2MigrationTool {

	public static final ParameterizedTypeReference<V2Page<V2Activity>> V_2_PAGE_PARAMETERIZED_TYPE_REFERENCE = new ParameterizedTypeReference<>() {};

	private final ObjectMapper objectMapper;

	@Autowired
	private ActivityRepository repository;// V3 repo

	private boolean stop;

	private final Logger logger = LoggerFactory.getLogger(getClass());

	public V2MigrationTool() {
		objectMapper = Jackson2ObjectMapperBuilder.json().featuresToDisable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();
	}

	@Async
	public void start(String v2Url, int startPage, Integer requestedEndPage) {
		stop = false;

		if (requestedEndPage == null) {
			requestedEndPage = Integer.MAX_VALUE;
		}

		final RestTemplate restTemplate = new RestTemplateBuilder().rootUri(v2Url).build();
		Integer adjustedEndPage = null;
		try {
			boolean keepLoading = true;
			int page = startPage;
			while (!stop && keepLoading) {
				final Map<String, Object> uriVariables = new HashMap<>();
				uriVariables.put("page", page++);
				final ResponseEntity<V2Page<V2Activity>> response = restTemplate.exchange("/activities?page={page}", HttpMethod.GET, null, V_2_PAGE_PARAMETERIZED_TYPE_REFERENCE,
						uriVariables);
				final V2Page<V2Activity> body = response.getBody();
				if (body != null) {
					if (adjustedEndPage == null) {
						adjustedEndPage = body.getTotalPages();
						if (requestedEndPage < adjustedEndPage) {
							adjustedEndPage = requestedEndPage;
						}
					}
					logger.info("Migrating v2 activity page {} (until {})", body.getNumber(), adjustedEndPage);
					readPage(body);
					keepLoading = body.getNumber() < adjustedEndPage;
				} else {
					logger.error("V2 API response is null. Status code:{}", response.getStatusCode());
					keepLoading = false;
				}
			}
		} catch (RestClientResponseException e) {
			logger.error("Call to v2 traceability API was not successful, status code:{}, message:{}", e.getRawStatusCode(), e.getResponseBodyAsString());
		}
		if (stop) {
			logger.info("Migration process stopped via API.");
		} else {
			logger.info("Migration of pages {} to {} complete.", startPage, adjustedEndPage);
		}
	}

	public void stop() {
		logger.info("Migration stop requested.");
		stop = true;
	}

	public void readPage(final V2Page<V2Activity> v2Activities) {
		v2Activities.getContent().forEach(v2Activity -> {
			if (!stop) {
				try {
					repository.save(new Activity(
							v2Activity.getUser().getUsername(),
							v2Activity.getBranch().getBranchPath(),
							v2Activity.getMergeSourceBranch() != null ? v2Activity.getMergeSourceBranch().getBranchPath() : null,
							v2Activity.getCommitDate(),
							ActivityType.valueOf(v2Activity.getActivityType())));
					System.out.print(".");
				} catch (Exception e) {
					logger.error("Failed to convert activity {} on page {}", v2Activity != null ? v2Activity.getId() : null, v2Activities.getNumber(), e);
				}
			}
		});
		System.out.println();
	}

	public ObjectMapper getObjectMapper() {
		return objectMapper;
	}
}
