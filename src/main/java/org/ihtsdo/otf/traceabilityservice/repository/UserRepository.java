package org.ihtsdo.otf.traceabilityservice.repository;

import org.snomed.otf.traceability.domain.User;
import org.springframework.data.repository.CrudRepository;

public interface UserRepository extends CrudRepository<User, Long> {

	User findByUsername(String username);

}
