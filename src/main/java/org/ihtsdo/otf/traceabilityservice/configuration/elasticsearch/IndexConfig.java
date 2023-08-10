package org.ihtsdo.otf.traceabilityservice.configuration.elasticsearch;

public record IndexConfig(String indexNamePrefix, short indexShards, short indexReplicas) {

	public IndexConfig {
		if (indexShards < 1) {
			throw new IllegalArgumentException("Number of index shards must be 1 or more.");
		}
	}


}
