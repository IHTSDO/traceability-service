package org.ihtsdo.otf.traceabilityservice.configuration;

import org.ihtsdo.sso.integration.RequestHeaderAuthenticationDecorator;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.web.access.intercept.FilterSecurityInterceptor;

@Configuration
@EnableWebSecurity
public class WebSecurityConfiguration extends WebSecurityConfigurerAdapter {

	@Override
	public void configure(HttpSecurity http) throws Exception {
		http.csrf().disable();

		http.addFilterBefore(new RequestHeaderAuthenticationDecorator(), FilterSecurityInterceptor.class);

		http.authorizeRequests()
				.antMatchers(
						"/version",
						"/swagger-ui/**",
						"/v3/api-docs/**").permitAll()
				.anyRequest().authenticated()
				.and().httpBasic();
	}
}
