/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.runtimeinfo;

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.common.testutils.GetterSetterTestHelper;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore.Status;

public class RuntimeInfoTest {
	RuntimeInfo runtimeInfo = new RuntimeInfo();

	/**
	 * Test getters and setters for all String and int fields.
	 */
	@Test
	public void testGettersAndSetters() {
		GetterSetterTestHelper.doTest(runtimeInfo, "runtimeId", "runtimeIdTest");
		GetterSetterTestHelper.doTest(runtimeInfo, "adaptorName", "adaptorNameTest");
		GetterSetterTestHelper.doTest(runtimeInfo, "entityName", "entityNameTest");
		GetterSetterTestHelper.doTest(runtimeInfo, "inputDescriptor", "inputDescriptorTest");
		GetterSetterTestHelper.doTest(runtimeInfo, "numOfAttempts", "numOfAttemptsTest");
	}

	/**
	 * Test getters and setters for status field.
	 */
	@Test
	public void testStatusGetterAndSetter() {
		Status status = Status.FAILED;
		runtimeInfo.setStatus(status);
		Assert.assertSame(Status.FAILED, runtimeInfo.getStatus());
	}

	/**
	 * Test getters and setters for status field.
	 */
	@Test
	public void testPropertiesGetterAndSetter() {
		Map<String, String> properties = new HashMap<>();
		runtimeInfo.setProperties(properties);
		Assert.assertSame(runtimeInfo.getProperties(), properties);
	}

	@Test
	public void testToString() {
		Assert.assertTrue(runtimeInfo.toString().startsWith("RuntimeInfo ["));
	}
}
