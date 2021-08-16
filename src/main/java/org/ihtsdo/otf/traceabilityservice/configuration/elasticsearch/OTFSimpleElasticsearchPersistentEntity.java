package org.ihtsdo.otf.traceabilityservice.configuration.elasticsearch;

import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.mapping.SimpleElasticsearchPersistentEntity;
import org.springframework.data.util.TypeInformation;

public class OTFSimpleElasticsearchPersistentEntity<T> extends SimpleElasticsearchPersistentEntity<T> {

	private final IndexConfig indexConfig;
	private final String indexNamePrefix;

	OTFSimpleElasticsearchPersistentEntity(IndexConfig indexConfig, TypeInformation<T> typeInformation) {
		super(typeInformation);
		this.indexConfig = indexConfig;
		this.indexNamePrefix = indexConfig.getIndexNamePrefix() != null ? indexConfig.getIndexNamePrefix() : "";
	}

	@Override
	public IndexCoordinates getIndexCoordinates() {
		return IndexCoordinates.of(indexNamePrefix + super.getIndexCoordinates().getIndexName());
	}

	@Override
	public short getShards() {
		return indexConfig.getIndexShards();
	}

	@Override
	public short getReplicas() {
		return indexConfig.getIndexReplicas();
	}
}
