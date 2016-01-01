/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.runtimeinfo;

import java.lang.reflect.InvocationTargetException;

import org.testng.annotations.Test;

import io.bigdime.common.testutils.ExceptionTester;

public class RuntimeInfoStoreExceptionTest extends ExceptionTester {
	@Test
	public void testConstructorWithString() throws InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		testConstructorWithString(RuntimeInfoStoreException.class);
	}

	@Test
	public void testConstructorWithStringAndThrowable() throws InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		testConstructorWithStringAndThrowable(RuntimeInfoStoreException.class);
	}

	@Test
	public void testConstructorWithThrowable() throws InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		testConstructorWithThrowable(RuntimeInfoStoreException.class);
	}

}
