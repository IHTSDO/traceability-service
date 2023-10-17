package org.ihtsdo.otf.traceabilityservice.service;

import org.ihtsdo.otf.traceabilityservice.util.QueryHelper;
import org.ihtsdo.otf.traceabilityservice.domain.*;
import org.ihtsdo.otf.traceabilityservice.repository.ActivityRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.client.elc.NativeQueryBuilder;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

@Service
public class PatchService {

	public static final String ROOT_CONCEPT = "138875005";
	public static final String HISTORY_PATCH_USERNAME = "history-patch";

	@Autowired
	private ElasticsearchOperations elasticsearchOperations;

	@Autowired
	private ActivityRepository activityRepository;

	public ChangeSummaryReport patchHistory(String branch, Set<String> componentsWithEffectiveTime, Set<String> componentsWithoutEffectiveTime) {

		final SearchHit<Activity> latestCommit = elasticsearchOperations.searchOne(new NativeQueryBuilder()
				.withQuery(QueryHelper.termQuery(Activity.Fields.branch, branch)).withSort(Sort.by(Activity.Fields.commitDate).descending()).build(), Activity.class);

		Date patchCommitDate = latestCommit != null ? latestCommit.getContent().getCommitDate() : new Date();

		activityRepository.save(new Activity(HISTORY_PATCH_USERNAME, branch, null, patchCommitDate, ActivityType.CONTENT_CHANGE).addConceptChange(new ConceptChange(ROOT_CONCEPT)
				.setComponentChanges(getComponentChanges(componentsWithEffectiveTime, componentsWithoutEffectiveTime))));

		return null;
	}

	private Set<ComponentChange> getComponentChanges(Set<String> componentsWithEffectiveTime, Set<String> componentsWithoutEffectiveTime) {
		Set<ComponentChange> changes = new HashSet<>();
		componentsWithEffectiveTime.forEach(id -> changes.add(new ComponentChange(id, ChangeType.UPDATE, ComponentIdUtil.getComponentType(id), null, false)));
		componentsWithoutEffectiveTime.forEach(id -> changes.add(new ComponentChange(id, ChangeType.UPDATE, ComponentIdUtil.getComponentType(id), null, true)));
		return changes;
	}

}
