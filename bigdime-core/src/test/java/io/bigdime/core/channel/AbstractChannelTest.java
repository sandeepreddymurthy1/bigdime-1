/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.channel;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AbstractChannelTest {

	@Test(expectedExceptions = NotImplementedException.class)
	public void testGetTransaction() {
		MemoryChannel memoryChannel = new MemoryChannel();
		memoryChannel.getTransaction();
	}

	@Test(expectedExceptions = NotImplementedException.class)
	public void testGetLifecycleState() {
		MemoryChannel memoryChannel = new MemoryChannel();
		memoryChannel.getLifecycleState();
	}

	@Test
	public void testPropertyMapGetterSetter() {
		MemoryChannel memoryChannel = new MemoryChannel();
		Map<String, Object> propertyMap = new HashMap<>();
		memoryChannel.setPropertyMap(propertyMap);
		Assert.assertSame(memoryChannel.getProperties(), propertyMap);
	}
}
