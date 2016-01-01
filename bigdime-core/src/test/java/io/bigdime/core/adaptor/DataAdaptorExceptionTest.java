/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.adaptor;

import java.lang.reflect.InvocationTargetException;

import org.testng.annotations.Test;

import io.bigdime.common.testutils.ExceptionTester;
import io.bigdime.core.DataAdaptorException;

public class DataAdaptorExceptionTest extends ExceptionTester {

	@Test
	public void testConstructorWithString() throws InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		testConstructorWithString(DataAdaptorException.class);
	}

	@Test
	public void testConstructorWithStringAndThrowable() throws InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		testConstructorWithStringAndThrowable(DataAdaptorException.class);
	}

	@Test
	public void testConstructorWithThrowable() throws InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		testConstructorWithThrowable(DataAdaptorException.class);
	}
}
