package org.ihtsdo.otf.traceabilityservice.service;

import org.ihtsdo.otf.traceabilityservice.domain.Activity;
import org.ihtsdo.otf.traceabilityservice.domain.ActivityType;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ActivityService {
	private static final Logger LOGGER = LoggerFactory.getLogger(ActivityService.class);

	private final ActivityRepository activityRepository;

	public ActivityService(ActivityRepository activityRepository) {
		this.activityRepository = activityRepository;
	}

	/**
	 * Return paged Activity matching query.
	 *
	 * @param conceptIds   Field to match in query.
	 * @param activityType Optional field to match in query.
	 * @param user         Optional field to match in query
	 * @param page         Page request.
	 * @return Paged Activity matching query.
	 */
	public Page<Activity> findBy(List<Long> conceptIds, ActivityType activityType, String user, Pageable page) {
		LOGGER.debug("Finding paged activities.");
		boolean noUser = user == null || user.isEmpty();
		boolean noActivityType = activityType == null;

		if (noUser && noActivityType) {
			LOGGER.debug("No user or activityType present. Finding by conceptIds only.");
			return activityRepository.findBy(conceptIds, page);
		}

		if (noUser) {
			LOGGER.debug("No user present. Finding by conceptIds and activityType only.");
			return activityRepository.findBy(conceptIds, activityType, page);
		}

		if (noActivityType) {
			LOGGER.debug("No activityType present. Finding by conceptIds and user only.");
			return activityRepository.findBy(conceptIds, user, page);
		}

		LOGGER.debug("Finding by conceptIds, activityType and user.");
		return activityRepository.findBy(conceptIds, activityType, user, page);
	}
}
