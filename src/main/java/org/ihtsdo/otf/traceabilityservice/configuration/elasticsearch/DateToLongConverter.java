package org.ihtsdo.otf.traceabilityservice.configuration.elasticsearch;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.WritingConverter;

import java.util.Date;

@WritingConverter
public class DateToLongConverter implements Converter<Date, Long> {

	@Override
	public Long convert(Date date) {
		return date.getTime();
	}

}
