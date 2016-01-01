/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core;

/**
 * This exception is thrown if the data adaptor or it's components can't be
 * built during startup.
 *
 * @author Neeraj Jain
 *
 */
public class AdaptorConfigurationException extends DataAdaptorException {

	private static final long serialVersionUID = 1L;

	public AdaptorConfigurationException(Throwable t) {
		super(t);
	}

	public AdaptorConfigurationException(String message) {
		super(message);
	}

	public AdaptorConfigurationException(String message, Throwable t) {
		super(message, t);
	}
}
