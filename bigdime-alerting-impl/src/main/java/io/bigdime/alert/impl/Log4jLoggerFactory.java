/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert.impl;

import io.bigdime.alert.Logger;
import io.bigdime.alert.spi.AlertLoggerFactory;

/**
 * Default implementation of the alerting system, uses slf4j-log4j to log.
 * 
 * @author Neeraj Jain
 *
 */

public class Log4jLoggerFactory implements AlertLoggerFactory {
	@Override
	public Logger getLogger(String name) {
		return Slf4jLogger.getLogger(name);
	}

	@Override
	public Logger getLogger(Class<?> clazz) {
		return Slf4jLogger.getLogger(clazz.getName());
	}
}
