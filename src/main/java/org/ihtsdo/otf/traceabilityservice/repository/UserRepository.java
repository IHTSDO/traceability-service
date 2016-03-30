package org.ihtsdo.otf.traceabilityservice.repository;

import org.ihtsdo.otf.traceabilityservice.domain.User;
import org.springframework.data.repository.CrudRepository;

public interface UserRepository extends CrudRepository<User, Long> {

	User findByUsername(String username);

}
