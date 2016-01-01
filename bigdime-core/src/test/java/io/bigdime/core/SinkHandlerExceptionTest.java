/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core;

import java.lang.reflect.InvocationTargetException;

import org.testng.annotations.Test;

import io.bigdime.common.testutils.ExceptionTester;

public class SinkHandlerExceptionTest extends ExceptionTester {

	@Test
	public void testConstructorWithString() throws InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		testConstructorWithString(SinkHandlerException.class);
	}

	@Test
	public void testConstructorWithStringAndThrowable() throws InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		testConstructorWithStringAndThrowable(SinkHandlerException.class);
	}
}
