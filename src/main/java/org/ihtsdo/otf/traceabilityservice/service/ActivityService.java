package org.ihtsdo.otf.traceabilityservice.service;

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.ihtsdo.otf.traceabilityservice.domain.Activity;
import org.ihtsdo.otf.traceabilityservice.domain.ActivityType;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

@Component
public class ActivityService {
	private static final Logger LOGGER = LoggerFactory.getLogger(ActivityService.class);

	private final ActivityRepository activityRepository;
	private final ElasticsearchOperations elasticsearchOperations;

	public ActivityService(ActivityRepository activityRepository, ElasticsearchOperations elasticsearchOperations) {
		this.activityRepository = activityRepository;
		this.elasticsearchOperations = elasticsearchOperations;
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

	public Page<Activity> getActivities(String originalBranch, String onBranch, ActivityType activityType, Long conceptId, Pageable page) {
		final BoolQueryBuilder query = boolQuery();

		if (originalBranch != null && !originalBranch.isEmpty()) {
			query.must(termQuery(Activity.Fields.branch, originalBranch));
		}
		if (onBranch != null && !onBranch.isEmpty()) {
			query.must(boolQuery()// One of these conditions must be true:
					// Either
					.should(termQuery(Activity.Fields.branch, onBranch))
					// Or
					.should(termQuery(Activity.Fields.highestPromotedBranch, onBranch)));
		}
		if (activityType != null) {
			query.must(termQuery(Activity.Fields.activityType, activityType));
		}
		if (conceptId != null) {
			query.must(termQuery(Activity.Fields.conceptChangesConceptId, conceptId));
		}

		final SearchHits<Activity> search = elasticsearchOperations.search(new NativeSearchQueryBuilder().withQuery(query).withPageable(page).build(), Activity.class);
		return new PageImpl<>(search.stream().map(SearchHit::getContent).collect(Collectors.toList()), page, search.getTotalHits());
	}
}
