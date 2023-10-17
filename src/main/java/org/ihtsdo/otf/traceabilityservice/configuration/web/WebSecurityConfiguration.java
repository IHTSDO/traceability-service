package org.ihtsdo.otf.traceabilityservice.configuration.web;

import org.ihtsdo.sso.integration.RequestHeaderAuthenticationDecorator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityCustomizer;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.intercept.AuthorizationFilter;
import org.springframework.security.web.firewall.DefaultHttpFirewall;
import org.springframework.security.web.firewall.HttpFirewall;

import static org.springframework.security.config.Customizer.withDefaults;

@Configuration
@EnableWebSecurity
public class WebSecurityConfiguration {
    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http.sessionManagement(sessionManagement -> sessionManagement.sessionCreationPolicy(SessionCreationPolicy.STATELESS));
        http.authorizeHttpRequests((authorize) ->
                authorize.requestMatchers(new String[]{
                                "/swagger-ui.html",
                                "/version",
                                "/swagger-ui/**",
                                "/v3/api-docs/**"
                        })
                        .permitAll()
                        .anyRequest()
                        .authenticated()
        );
        http.httpBasic(withDefaults());
        http.csrf(AbstractHttpConfigurer::disable);
        http.addFilterBefore(new RequestHeaderAuthenticationDecorator(), AuthorizationFilter.class);

        return http.build();
    }

    @Bean
    public WebSecurityCustomizer webSecurityCustomizer() {
        return (web) -> web.httpFirewall(allowUrlEncodedSlashHttpFirewall());
    }

    @Bean
    public HttpFirewall allowUrlEncodedSlashHttpFirewall() {
        DefaultHttpFirewall firewall = new DefaultHttpFirewall();
        firewall.setAllowUrlEncodedSlash(true);
        return firewall;
    }
}
