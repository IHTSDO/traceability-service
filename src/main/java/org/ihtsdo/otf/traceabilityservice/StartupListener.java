package org.ihtsdo.otf.traceabilityservice;

import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;

@Component
public class StartupListener implements CommandLineRunner {
    private final ActivityRepository activityRepository;

    public StartupListener(ActivityRepository activityRepository) {
        this.activityRepository = activityRepository;
    }

    @Override
    public void run(String... args) {
        checkElasticsearchConnection();
    }

    public void checkElasticsearchConnection() {
        try {
            activityRepository.findAll(PageRequest.of(0, 1));
        } catch (DataAccessResourceFailureException e) {
            throw new IllegalStateException("Failed to connect to Elasticsearch.", e);
        }
    }
}
