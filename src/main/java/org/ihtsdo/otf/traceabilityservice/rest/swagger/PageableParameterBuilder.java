package org.ihtsdo.otf.traceabilityservice.rest.swagger;

import com.fasterxml.classmate.TypeResolver;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.MethodParameter;
import org.springframework.core.annotation.Order;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;
import springfox.documentation.schema.ModelRef;
import springfox.documentation.service.Parameter;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spi.service.ParameterBuilderPlugin;
import springfox.documentation.spi.service.contexts.ParameterContext;
import springfox.documentation.swagger.common.SwaggerPluginSupport;
import static com.google.common.collect.Lists.newArrayList;

@Component
@Order(SwaggerPluginSupport.SWAGGER_PLUGIN_ORDER)
public class PageableParameterBuilder implements ParameterBuilderPlugin {

	@Override
	public boolean supports(DocumentationType delimiter) {
		return DocumentationType.SWAGGER_2.equals(delimiter);
	}

	@Override
	public void apply(ParameterContext parameterContext) {
		MethodParameter param = parameterContext.methodParameter();
		if (param.getParameterType().equals(Pageable.class)) {
			List<Parameter> parameters = newArrayList();
			parameters.add(parameterContext.parameterBuilder()
					.parameterType("query").name("page").modelRef(new ModelRef("int"))
					.description("Page number (zero based).").build());
			parameters.add(parameterContext.parameterBuilder()
					.parameterType("query").name("size").modelRef(new ModelRef("int"))
					.description("Page size.").build());
			parameters.add(parameterContext.parameterBuilder()
					.parameterType("query").name("sort").modelRef(new ModelRef("string")).allowMultiple(true)
					.description("Sorting criteria. e.g. \"property\" or \"property,desc\". "
							+ "Multiple sort criteria are supported.").build());
			parameterContext.getOperationContext().operationBuilder().parameters(parameters);
		}
	}
}
