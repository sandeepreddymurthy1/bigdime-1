/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.testutils;

import java.lang.reflect.InvocationTargetException;

import org.testng.Assert;

public class ExceptionTester {
	public void testConstructorWithString(Class<? extends Exception> clazz)
			throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		Exception ex = clazz.getDeclaredConstructor(String.class).newInstance(clazz.getName());
		Assert.assertNotNull(ex, "Valid Exception object must be created");
		Assert.assertEquals(ex.getMessage(), clazz.getName());
	}

	public void testConstructorWithThrowable(Class<? extends Exception> clazz)
			throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		Throwable t = new Throwable();
		Exception ex = clazz.getDeclaredConstructor(Throwable.class).newInstance(t);
		Assert.assertNotNull(ex, "Valid Exception object must be created");
		Assert.assertSame(ex.getCause(), t);
	}

	public void testConstructorWithStringAndThrowable(Class<? extends Exception> clazz)
			throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		Throwable t = new Throwable();
		Exception ex = clazz.getDeclaredConstructor(String.class, Throwable.class)
				.newInstance("unit-" + clazz.getName(), t);
		Assert.assertNotNull(ex, "Valid Exception object must be created");
		Assert.assertEquals(ex.getMessage(), "unit-" + clazz.getName());
		Assert.assertSame(ex.getCause(), t);
	}

}
