/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert;

import java.util.Iterator;
import java.util.ServiceLoader;

import io.bigdime.alert.spi.AlertLoggerFactory;

/**
 * Factory for getting the {@link Logger} instance based on String or Class.
 * 
 * @author Neeraj Jain
 *
 */
public final class LoggerFactory {
	private static LoggerFactory instance = new LoggerFactory();
	private ServiceLoader<AlertLoggerFactory> loggerServiceLoader;
	private AlertLoggerFactory loggerFactory;

	private LoggerFactory() {
		loggerServiceLoader = ServiceLoader.load(AlertLoggerFactory.class);
		final Iterator<AlertLoggerFactory> loggerFactoryIterator = loggerServiceLoader.iterator();

		System.out.println("Finding bindings for AlertLoggerFactory");
		while (loggerFactoryIterator.hasNext()) {
			if (loggerFactory == null) {
				loggerFactory = loggerFactoryIterator.next();
				System.out.println("AlertLoggerFactory binding found:" + loggerFactory);
			} else {
				System.err.println(
						"multiple bindings found for AlertLoggerFactory, skipping:" + loggerFactoryIterator.next());
			}
		}
		setupUnavailableLoggerFactory();
	}

	/**
	 * UnavailableLoggerFactory setup.
	 */
	private void setupUnavailableLoggerFactory() {
		if (loggerFactory == null) {
			System.err.println(
					"no binding found : setupUnavailableLoggerFactory"  );
			loggerFactory = UnavailableLoggerFactory.getInstance();
		}
	}

	public static LoggerFactory getInstance() {
		return instance;
	}

	/**
	 * Get the instance of Logger for given key specified by name.
	 * 
	 * @param name
	 *            the logger to be named after name
	 * @return logger
	 */
	public static Logger getLogger(String name) {
		return instance.loggerFactory.getLogger(name);
	}

	/**
	 * Get the instance of Logger for given Class.
	 * 
	 * @param clazz
	 *            the logger to be named after clazz
	 * @return logger
	 */
	public static Logger getLogger(Class<?> clazz) {
		return instance.loggerFactory.getLogger(clazz);
	}
}
