/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.config;

public enum ADAPTOR_TYPE {
	/**
	 * Adaptors that process the data from a stream, say kafka, jms etc.
	 */
	STREAMING("streaming"),

	/**
	 * Adaptors that receive the data in batches, say a file or database etc
	 */
	BATCH("batch");

	private String value;

	private ADAPTOR_TYPE(final String value) {
		this.value = value;
	}

	public static ADAPTOR_TYPE getByValue(String value) {
		if (value == null) {
			return null;
		}
		for (ADAPTOR_TYPE type : ADAPTOR_TYPE.values()) {
			if (type.value.equals(value)) {
				return type;
			}
		}
		return null;
	}
}