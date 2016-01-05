/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.testutils.test;

import io.bigdime.common.testutils.TestUtils;
import static io.bigdime.common.testutils.test.TestUtilsConstants.*;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.net.ServerSocket;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * 
 * @author mnamburi
 *
 */
public final class TestUtilsTest {

	@Test()
	public void testfindAvailablePort() throws IOException {
			int availablePort = TestUtils.findAvailablePort(zeroPort);
			Assert.assertNotEquals(0, availablePort);
	}
	
	@Test
	public void testPrivateTestUtils() throws Exception{
	    final Constructor<?>[] constructors = TestUtils.class.getDeclaredConstructors();
	    for (Constructor<?> constructor : constructors) {
	        Assert.assertTrue(Modifier.isPrivate(constructor.getModifiers()));
	        constructor.setAccessible(true);
	        Assert.assertNotNull(constructor.newInstance());
	    }
	}
	
	@Test(expectedExceptions=IOException.class)
	public void testCheckedException() throws IOException{
		int availablePort = TestUtils.findAvailablePort(zeroPort);
		ServerSocket serverSocket = new ServerSocket(availablePort);
		Assert.assertEquals(serverSocket.getLocalPort(), availablePort);
		TestUtils.findAvailablePort(availablePort);
	}
}
