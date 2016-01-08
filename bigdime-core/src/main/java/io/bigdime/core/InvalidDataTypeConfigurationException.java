/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core;

public class InvalidDataTypeConfigurationException extends AdaptorConfigurationException {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	public InvalidDataTypeConfigurationException(String parameterName, String expectedDataType) {
		super(parameterName + " must be of " + expectedDataType);
	}

}
