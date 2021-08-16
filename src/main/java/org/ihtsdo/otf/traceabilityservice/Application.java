package org.ihtsdo.otf.traceabilityservice;

import org.ihtsdo.otf.traceabilityservice.configuration.Config;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jms.annotation.EnableJms;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import static com.google.common.base.Predicates.not;
import static springfox.documentation.builders.PathSelectors.regex;

@EnableJms
@EnableSwagger2
public class Application extends Config {

	public static final String TRACEABILITY_QUEUE_SUFFIX = "traceability";

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Bean
	public Docket api() {
		return new Docket(DocumentationType.SWAGGER_2)
						.select()
						.apis(RequestHandlerSelectors.any())
						.paths(not(regex("/")))
						.paths(not(regex("/error")))
						.build();
	}

}
