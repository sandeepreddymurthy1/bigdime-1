/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert.spi;

import io.bigdime.alert.Logger;

/**
 * Service providers to implement this interface to provide their own version of
 * alerting system.
 * 
 * @author Neeraj Jain
 *
 */
public interface AlertLoggerFactory {

	/**
	 * Gets the {@link Logger} for the given logger name.
	 * 
	 * @param name
	 *            the logger will be named after name
	 * @return instance of {@link Logger}
	 */
	public Logger getLogger(String name);

	/**
	 * Gets the {@link Logger} for the given class.
	 * 
	 * @param clazz
	 *            the logger will be named after clazz
	 * @return instance of {@link Logger}
	 */
	public Logger getLogger(Class<?> clazz);
}
