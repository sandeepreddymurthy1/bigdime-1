/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.testutils;

import java.lang.reflect.InvocationTargetException;

import org.testng.annotations.Test;

public class ExceptionTesterTest {

	@Test
	public void testConstructorWithString() throws InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		ExceptionTester exceptionTester = new ExceptionTester();
		exceptionTester.testConstructorWithString(DummyException.class);
	}

	@Test
	public void testConstructorWithStringAndThrowable() throws InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		ExceptionTester exceptionTester = new ExceptionTester();
		exceptionTester.testConstructorWithThrowable(DummyException.class);
	}

	@Test
	public void testConstructorWithThrowable() throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		ExceptionTester exceptionTester = new ExceptionTester();
		exceptionTester.testConstructorWithStringAndThrowable(DummyException.class);
	}
}
