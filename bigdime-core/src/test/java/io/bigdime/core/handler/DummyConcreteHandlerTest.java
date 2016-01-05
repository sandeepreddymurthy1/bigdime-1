/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.bigdime.core.Handler;

public class DummyConcreteHandlerTest {

	DummyConcreteHandler dummyConcreteHandler = null;

	@BeforeMethod
	public void init() {
		dummyConcreteHandler = new DummyConcreteHandler();
	}

	@Test
	public void testGetName() {
		ReflectionTestUtils.setField(dummyConcreteHandler, "name", "unit-test-name");
		Assert.assertEquals(dummyConcreteHandler.getName(), "unit-test-name",
				"getName() should return the actual name of the handler");

	}

	@Test
	public void testSetName() {
		dummyConcreteHandler.setName("unit-test-name");
		Assert.assertEquals(dummyConcreteHandler.getName(), "unit-test-name",
				"getName() should return the name set using setName() method");
	}

	@Test
	public void testGetId() {
		Assert.assertNotNull(dummyConcreteHandler.getId(), "id should be not null");
	}

	@Test
	public void testGetState() {
		Assert.assertNull(dummyConcreteHandler.getState(), "state should be not null");
	}

	@Test
	public void testShutdown() {
		dummyConcreteHandler.shutdown();
		Assert.assertEquals(dummyConcreteHandler.getState(), Handler.State.TERMINATED, "state should be TERMINATED");
	}

}
