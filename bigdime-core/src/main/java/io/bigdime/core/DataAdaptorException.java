/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core;

/**
 * This is general exception that data adaptor and its components can throw when
 * they encounter an exception condition.
 *
 * @author Neeraj Jain
 *
 */

public class DataAdaptorException extends Exception {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	public DataAdaptorException(Throwable t) {
		super(t);
	}

	public DataAdaptorException(String message) {
		super(message);
	}

	public DataAdaptorException(String message, Throwable t) {
		super(message, t);
	}

}
