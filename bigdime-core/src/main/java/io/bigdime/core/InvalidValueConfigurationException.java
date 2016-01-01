/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core;

/**
 * This exception to be thrown if the value provided in the configuration is
 * invalid, e.g. the value is expected a positive number but a negative value is
 * provided instead.
 * 
 * @author Neeraj Jain
 *
 */
public class InvalidValueConfigurationException extends AdaptorConfigurationException {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	public InvalidValueConfigurationException(String message) {
		super(message);
	}

}
