package org.ihtsdo.otf.traceabilityservice.configuration.elasticsearch;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import javax.validation.constraints.NotNull;
import java.util.Date;

@ReadingConverter
public class LongToDateConverter implements Converter<Long, Date> {

	@Override
	public Date convert(@NonNull Long dateInMillis) {
		return new Date(dateInMillis);
	}

}
