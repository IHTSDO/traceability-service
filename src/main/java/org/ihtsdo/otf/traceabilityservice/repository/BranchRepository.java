package org.ihtsdo.otf.traceabilityservice.repository;

import java.util.List;
import java.util.Set;

import org.snomed.otf.traceability.domain.Branch;
import org.springframework.data.repository.CrudRepository;

public interface BranchRepository extends CrudRepository<Branch, Long> {

	Branch findByBranchPath(String branchPath);
	
	List<Branch> findByBranchPathIn (Set<String> branchPath);
	
}
