package org.ihtsdo.otf.traceabilityservice.configuration;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.web.PageableHandlerMethodArgumentResolver;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import java.util.List;

@Configuration
public class WebConfiguration extends WebMvcConfigurerAdapter {

	public static final int DEFAULT_PAGE_SIZE = 100;

	@Override
	public void addArgumentResolvers(List<HandlerMethodArgumentResolver> argumentResolvers) {
		PageableHandlerMethodArgumentResolver resolver = new PageableHandlerMethodArgumentResolver();
		resolver.setFallbackPageable(new PageRequest(0, DEFAULT_PAGE_SIZE));
		argumentResolvers.add(resolver);
		super.addArgumentResolvers(argumentResolvers);
	}

}
