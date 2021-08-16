package org.ihtsdo.otf.traceabilityservice.configuration.elasticsearch;

import org.springframework.data.elasticsearch.core.mapping.SimpleElasticsearchMappingContext;
import org.springframework.data.elasticsearch.core.mapping.SimpleElasticsearchPersistentEntity;
import org.springframework.data.util.TypeInformation;

public class OTFElasticsearchMappingContext extends SimpleElasticsearchMappingContext {

	private final IndexConfig indexConfig;

	public OTFElasticsearchMappingContext(IndexConfig indexConfig) {
		this.indexConfig = indexConfig;
	}

	@Override
	protected <T> SimpleElasticsearchPersistentEntity<?> createPersistentEntity(TypeInformation<T> typeInformation) {
		return new OTFSimpleElasticsearchPersistentEntity<>(indexConfig, typeInformation);
	}

}
