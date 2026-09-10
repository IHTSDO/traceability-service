package org.ihtsdo.otf.traceabilityservice.configuration.elasticsearch;

import org.jspecify.annotations.NonNull;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;

import java.util.Date;

@ReadingConverter
public class LongToDateConverter implements Converter<Long, Date> {

	@Override
	public Date convert(@NonNull Long dateInMillis) {
		return new Date(dateInMillis);
	}

}
