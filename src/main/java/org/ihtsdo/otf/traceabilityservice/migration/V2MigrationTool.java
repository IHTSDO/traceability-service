package org.ihtsdo.otf.traceabilityservice.migration;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import org.ihtsdo.otf.traceabilityservice.domain.*;
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

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// Tool for migrating from Traceability Service version 2 to version 3
@Service
public class V2MigrationTool {

	public static final ParameterizedTypeReference<V2Page<V2Activity>> V_2_PAGE_PARAMETERIZED_TYPE_REFERENCE = new ParameterizedTypeReference<>() {};

	@Value("${migration.save-batch-size}")
	private int saveBatchSize;

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
			logger.info("Starting migration process using save batch size {}...", saveBatchSize);
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
					logger.info("Migrating v2 activity page {} (will stop at page {})", body.getNumber(), adjustedEndPage);
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
		final List<Activity> v3Activities = v2Activities.getContent().stream()
				.map(v2Activity -> new Activity(
						v2Activity.getUser().getUsername(),
						v2Activity.getBranch().getBranchPath(),
						v2Activity.getMergeSourceBranch() != null ? v2Activity.getMergeSourceBranch().getBranchPath() : null,
						v2Activity.getCommitDate(),
						ActivityType.valueOf(v2Activity.getActivityType()))
						.setConceptChanges(convertChanges(v2Activity.getConceptChanges())))
				.collect(Collectors.toList());
		for (List<Activity> v3ActivityBatch : Iterables.partition(v3Activities, saveBatchSize)) {
			if (!stop) {
				try {
					long componentChangeCount = v3Activities.stream()
							.flatMap(activity -> activity.getConceptChanges() != null ? activity.getConceptChanges().stream() : Stream.empty())
							.mapToLong(change -> change.getComponentChanges().size())
							.sum();
					if (componentChangeCount > saveBatchSize * 15L) {
						logger.info("Component count within this batch is more than 15 times the batch size, saving as 10 parts.");
						// Pages are unusually large. Split batches further to avoid Elasticsearch request too large error.
						for (List<Activity> subBatch : Iterables.partition(v3Activities, 10)) {
							repository.saveAll(subBatch);
						}
					} else {
						// saveAll() uses "POST _bulk" endpoint which is blocked by AWS security policies using index name prefix.
						// We are relaxing this policy just during our migration.
						repository.saveAll(v3ActivityBatch);
					}
				} catch (Exception e) {
					logger.error("Failed to save batch of activities on page {}", v2Activities.getNumber(), e);
				}
			}
		}
	}

	private Set<ConceptChange> convertChanges(List<V2Activity.V2ConceptChange> conceptChanges) {
		if (conceptChanges == null) {
			return null;
		}
		return conceptChanges.stream()
				.filter(v2ConceptChange -> isLong(v2ConceptChange.getConceptId()))
				.map(v2ConceptChange -> {
					final ConceptChange conceptChange = new ConceptChange(v2ConceptChange.getConceptId());
					for (V2Activity.V2ComponentChange v2Change : v2ConceptChange.getComponentChanges()) {
						conceptChange.addComponentChange(getComponentChange(v2Change));
					}
					return conceptChange;
				})
				.collect(Collectors.toSet());
	}

	private ComponentChange getComponentChange(V2Activity.V2ComponentChange v2Change) {

		ComponentType componentType = null;
		Long componentSubType = null;

		String v2ComponentType = v2Change.getComponentType();
		/// V2 = CONCEPT, DESCRIPTION, RELATIONSHIP, OWLAXIOM, REFERENCESETMEMBER
		if (v2ComponentType != null) {
			if (v2ComponentType.equals("REFERENCESETMEMBER")) {
				v2ComponentType = "REFERENCE_SET_MEMBER";
				// No sub-type information is available for these entries, will have to leave blank
			} else if (v2ComponentType.equals("OWLAXIOM")) {
				// 733073007 | OWL axiom reference set (foundation metadata concept) |
				componentSubType = 733073007L;
				v2ComponentType = "REFERENCE_SET_MEMBER";
			}
			componentType = ComponentType.valueOf(v2ComponentType);
		}

		// STATED_RELATIONSHIP, INFERRED_RELATIONSHIP, FSN_DESCRIPTION, SYNONYM_DESCRIPTION
		final String v2ComponentSubType = v2Change.getComponentSubType();
		if (v2ComponentSubType != null) {
			switch (v2ComponentSubType) {
				case "STATED_RELATIONSHIP":
					// 900000000000010007 | Stated relationship (core metadata concept) |
					componentSubType = 900000000000010007L;
					break;
				case "INFERRED_RELATIONSHIP":
					// 900000000000011006 | Inferred relationship (core metadata concept) |
					componentSubType = 900000000000011006L;
					break;
				case "FSN_DESCRIPTION":
					// 900000000000003001 | Fully specified name (core metadata concept) |
					componentSubType = 900000000000003001L;
					break;
				case "SYNONYM_DESCRIPTION":
					// 900000000000013009 | Synonym (core metadata concept) |
					componentSubType = 900000000000013009L;
					break;
				default:
					componentSubType = null;
					break;
			}
			// There was no sub-type for text definition in the old V2 code.
		}

		return new ComponentChange(v2Change.getComponentId(), ChangeType.valueOf(v2Change.getChangeType()),
				componentType, componentSubType != null ? componentSubType.toString() : null, true);
	}

	private boolean isLong(String string) {
		return string != null && !string.isEmpty() && string.matches("[0-9]+");
	}

	public ObjectMapper getObjectMapper() {
		return objectMapper;
	}
}
