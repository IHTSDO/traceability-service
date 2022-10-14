package org.ihtsdo.otf.traceabilityservice.migration;

import org.ihtsdo.otf.traceabilityservice.domain.*;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

@Service
public class TraceabilityDataSimulator {

	@Autowired
	private ActivityRepository activityRepository;

	private final long concept = 395317004L;

	public void generateData(int max, String branch) {
		// To simulate large batch of activity changes
		System.out.println("Start");
		Activity activity = new Activity("mchu", branch, branch, new Date(), ActivityType.CONTENT_CHANGE);
		Set<ConceptChange> conceptChanges = simulateChanges(max);
		activity.setConceptChanges(conceptChanges);
		activityRepository.save(activity);
		System.out.println("End");
	}

	private Set<ConceptChange> simulateChanges(int max) {
		Set<ConceptChange> results = new HashSet<>();
		ConceptChange conceptChange = new ConceptChange(String.valueOf(concept));
		for (int i = 0; i < max; i++) {
			conceptChange.addComponentChange(new ComponentChange(UUID.randomUUID().toString(), ChangeType.CREATE,
					ComponentType.REFERENCE_SET_MEMBER, null, true));
		}
		System.out.println("Total component changes " + conceptChange.getComponentChanges().size());
		results.add(conceptChange);
		return results;
	}
}
