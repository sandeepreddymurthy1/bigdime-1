/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core;

import java.lang.reflect.InvocationTargetException;

import org.testng.annotations.Test;

import io.bigdime.common.testutils.ExceptionTester;

public class AdaptorConfigurationExceptionTest extends ExceptionTester {
	@Test
	public void testConstructorWithString() throws InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		testConstructorWithString(AdaptorConfigurationException.class);
	}

	@Test
	public void testConstructorWithStringAndThrowable() throws InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		testConstructorWithStringAndThrowable(AdaptorConfigurationException.class);
	}

	@Test
	public void testConstructorWithThrowable() throws InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		testConstructorWithThrowable(AdaptorConfigurationException.class);
	}
}
