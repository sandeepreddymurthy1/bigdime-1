/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core;

public class RequiredParameterMissingConfigurationException extends AdaptorConfigurationException {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	public RequiredParameterMissingConfigurationException(String parameterName) {
		super(parameterName + " not specified");
	}

}
