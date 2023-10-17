package org.ihtsdo.otf.traceabilityservice.configuration.elasticsearch;

import org.ihtsdo.otf.traceabilityservice.configuration.ApplicationProperties;
import org.springframework.stereotype.Component;

@Component
public class IndexNameProvider {
    private final ApplicationProperties applicationProperties;

    public IndexNameProvider(ApplicationProperties applicationProperties) {
        this.applicationProperties = applicationProperties;
    }

    public String getIndexNameWithPrefix(String indexName) {
        String prefix = "";

        String indexPrefix = applicationProperties.getIndexPrefix();
        if (indexPrefix != null && !indexPrefix.isEmpty()) {
            prefix = prefix + indexPrefix;
        }

        String applicationPrefix = applicationProperties.getIndexApplicationPrefix();
        if (applicationPrefix != null && !applicationPrefix.isEmpty()) {
            prefix = prefix + applicationPrefix;
        }

        if (prefix.isEmpty()) {
            return indexName;
        }

        return prefix;
    }
}
