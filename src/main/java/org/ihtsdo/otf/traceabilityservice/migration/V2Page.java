package org.ihtsdo.otf.traceabilityservice.migration;

import java.util.List;

public class V2Page<T> {

	private Integer number;
	private Integer totalPages;
	private List<T> content;

	public Integer getNumber() {
		return number;
	}

	public Integer getTotalPages() {
		return totalPages;
	}

	public List<T> getContent() {
		return content;
	}
}
