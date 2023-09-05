package org.ihtsdo.otf.traceabilityservice;

import io.swagger.v3.oas.models.ExternalDocumentation;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.ihtsdo.otf.traceabilityservice.configuration.Config;
import org.springdoc.core.GroupedOpenApi;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableJms
@EnableAsync
public class Application extends Config {
	@Autowired(required = false)
	private BuildProperties buildProperties;

	public static final String TRACEABILITY_QUEUE_SUFFIX = "traceability";

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Bean
	public GroupedOpenApi apiDocs() {
		return GroupedOpenApi.builder()
				.group("traceability-service")
				.packagesToScan("org.ihtsdo.otf.traceabilityservice.rest")
				// Don't show the migration, error or root endpoints in this group
				.pathsToExclude("/migration/**", "/patch/**", "/error", "/")
				.build();
	}

	@Bean
	public GroupedOpenApi migrationApi() {
		return GroupedOpenApi.builder()
				.group("migration")
				.packagesToScan("org.ihtsdo.otf.traceabilityservice.rest")
				.pathsToMatch("/migration/**")
				.build();
	}

	@Bean
	public GroupedOpenApi patchApi() {
		return GroupedOpenApi.builder()
				.group("patch")
				.packagesToScan("org.ihtsdo.otf.traceabilityservice.rest")
				.pathsToMatch("/patch/**")
				.build();
	}

	@Bean
	public GroupedOpenApi springActuatorApi() {
		return GroupedOpenApi.builder()
				.group("actuator")
				.packagesToScan("org.springframework.boot.actuate")
				.pathsToMatch("/actuator/**")
				.build();
	}

	@Bean
	public OpenAPI apiInfo() {
		final String version = buildProperties != null ? buildProperties.getVersion() : "DEV";
		return new OpenAPI()
				.info(new Info()
						.title("SNOMED CT Traceability Service")
						.description("Standalone service to consume SNOMED CT content authoring events and expose them over a REST API.")
						.version(version)
						.contact(new Contact().name("SNOMED International").url("https://www.snomed.org"))
						.license(new License().name("Apache 2.0").url("http://www.apache.org/licenses/LICENSE-2.0")))
				.externalDocs(new ExternalDocumentation()
						.description("See more about Traceability Service in GitHub")
						.url("https://github.com/IHTSDO/traceability-service"));
	}

}
