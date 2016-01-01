/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.channel;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.NotImplementedException;
import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.config.AdaptorConfigConstants.ChannelConfigConstants;

public class MemoryChannelTest {

	@Test
	public void testBuild() {
		MemoryChannel memoryChannel = new MemoryChannel();
		Map<String, Object> propertyMap = new HashMap<>();// Mockito.mock(Map.class);
		propertyMap.put(ChannelConfigConstants.PRINT_STATS, "true");
		propertyMap.put(ChannelConfigConstants.PRINT_STATS_DURATION_IN_SECONDS, "10d");
		memoryChannel.setPropertyMap(propertyMap);
		memoryChannel.build();
	}

	/**
	 * If there are more than one consumers registered with the channel, a
	 * request to take an event must come from a valid consumer.
	 */
	@Test(expectedExceptions = UnsupportedOperationException.class)
	public void testTakeOneEventWithMultipleConsumers() {
		MemoryChannel memoryChannel = new MemoryChannel();
		Set<String> consumerNames = new HashSet<>();
		consumerNames.add("c1");

		ReflectionTestUtils.setField(memoryChannel, "channelCapacity", 1000);
		ReflectionTestUtils.setField(memoryChannel, "consumerNames", consumerNames);

		ActionEvent putEvent = new ActionEvent();

		putEvent.setBody(UUID.randomUUID().toString().getBytes(Charset.defaultCharset()));
		memoryChannel.put(putEvent);
		Event took = memoryChannel.take();
		Assert.assertNotNull(took);
		Assert.assertSame(took, putEvent);
	}

	/**
	 * If there are more than one consumers registered with the channel, a
	 * request to take an event must come from a valid consumer.
	 */
	@Test(expectedExceptions = UnsupportedOperationException.class)
	public void testTakeMultipleEventsWithMultipleConsumers() {
		MemoryChannel memoryChannel = new MemoryChannel();
		Set<String> consumerNames = new HashSet<>();
		consumerNames.add("c1");

		ReflectionTestUtils.setField(memoryChannel, "channelCapacity", 1000);
		ReflectionTestUtils.setField(memoryChannel, "consumerNames", consumerNames);

		ActionEvent putEvent = new ActionEvent();
		putEvent.setBody(UUID.randomUUID().toString().getBytes(Charset.defaultCharset()));
		memoryChannel.put(putEvent);
		List<Event> tookEvents = memoryChannel.take(1);
		Assert.assertNotNull(tookEvents);
		Assert.assertEquals(tookEvents.size(), 1);
		Assert.assertSame(tookEvents.get(0), putEvent);
	}

	/**
	 * If there is no data on the channel, ChannelException must be thrown.
	 */
	@Test(expectedExceptions = ChannelException.class)
	public void testTakeWithNoDataOnChannel() {
		MemoryChannel memoryChannel = new MemoryChannel();
		memoryChannel.take();
		Assert.fail("ChannelException must have been thrown.");
	}

	/**
	 * Test that events put and take work well for a default consumer.
	 */
	@Test
	public void testTakeOneEvent() {

		MemoryChannel memoryChannel = new MemoryChannel();

		ActionEvent event1 = new ActionEvent();
		event1.setBody("i0".getBytes(Charset.defaultCharset()));
		ActionEvent event2 = new ActionEvent();
		event2.setBody("i1".getBytes(Charset.defaultCharset()));
		ActionEvent event3 = new ActionEvent();
		event3.setBody("i2".getBytes(Charset.defaultCharset()));
		ReflectionTestUtils.setField(memoryChannel, "channelCapacity", 1000);
		memoryChannel.put(event1);
		memoryChannel.put(event2);
		memoryChannel.put(event3);

		Assert.assertSame(memoryChannel.take(), event1);
		Assert.assertSame(memoryChannel.take(), event2);
		Assert.assertSame(memoryChannel.take(), event3);
	}

	/**
	 * Test when 3 events are put on the channel and they are taken in multiple
	 * calls, the order is maintained.
	 */
	@Test
	public void testTakeMoreThanOneEvents() {

		MemoryChannel memoryChannel = new MemoryChannel();

		ActionEvent event1 = new ActionEvent();
		event1.setBody("i0".getBytes(Charset.defaultCharset()));
		ActionEvent event2 = new ActionEvent();
		event2.setBody("i1".getBytes(Charset.defaultCharset()));
		ActionEvent event3 = new ActionEvent();
		event3.setBody("i2".getBytes(Charset.defaultCharset()));
		ReflectionTestUtils.setField(memoryChannel, "channelCapacity", 1000);
		memoryChannel.put(event1);
		memoryChannel.put(event2);
		memoryChannel.put(event3);

		List<Event> firstTwoEvents = memoryChannel.take(2);
		Assert.assertEquals(firstTwoEvents.size(), 2);
		Assert.assertEquals(firstTwoEvents.get(0), event1);
		Assert.assertEquals(firstTwoEvents.get(1), event2);
		List<Event> thirdEvent = memoryChannel.take(2);
		Assert.assertEquals(thirdEvent.size(), 1);
		Assert.assertEquals(thirdEvent.get(0), event3);
	}

	/**
	 * Test the most complex scenario, where there are more than one consumers
	 * present and they consume data in arbitrary order.
	 */
	@Test
	public void testTakeWithMultipleConsumers() {
		MemoryChannel memoryChannel = new MemoryChannel();
		Set<String> consumerNames = new HashSet<>();
		consumerNames.add("c1");
		consumerNames.add("c2");

		ReflectionTestUtils.setField(memoryChannel, "channelCapacity", 1000);
		ReflectionTestUtils.setField(memoryChannel, "consumerNames", consumerNames);

		ActionEvent event1 = new ActionEvent();
		event1.setBody("i1".getBytes(Charset.defaultCharset()));
		ActionEvent event2 = new ActionEvent();
		event2.setBody("i2".getBytes(Charset.defaultCharset()));
		ActionEvent event3 = new ActionEvent();
		event3.setBody("i3".getBytes(Charset.defaultCharset()));
		ActionEvent event4 = new ActionEvent();
		event4.setBody("i4".getBytes(Charset.defaultCharset()));
		ActionEvent event5 = new ActionEvent();
		event5.setBody("i5".getBytes(Charset.defaultCharset()));
		memoryChannel.put(event1);
		memoryChannel.put(event2);
		memoryChannel.put(event3);
		memoryChannel.put(event4);
		memoryChannel.put(event5);

		List<Event> e1 = memoryChannel.take("c1", 2);
		Assert.assertEquals(e1.size(), 2);
		Assert.assertSame(e1.get(0), event1);
		Assert.assertSame(e1.get(1), event2);
		Event e2 = memoryChannel.take("c1");
		Assert.assertSame(e2, event3);

		Event e3 = memoryChannel.take("c2");
		Assert.assertSame(e3, event1);

		List<Event> e4 = memoryChannel.take("c1", 8);
		Assert.assertEquals(e4.size(), 2);
		Assert.assertSame(e4.get(0), event4);
		Assert.assertSame(e4.get(1), event5);

		List<Event> e5 = memoryChannel.take("c2", 0);
		Assert.assertEquals(e5.size(), 0);

		List<Event> e6 = memoryChannel.take("c2", 5);
		Assert.assertEquals(e6.size(), 4);
	}

	/**
	 * Make sure that start method is implemented and doesn't throw any
	 * exception.
	 * 
	 * @throws Throwable
	 */
	@Test
	public void testStart() throws Throwable {
		// no exception is good enough
		Thread t = new Thread() {
			@Override
			public void run() {
				MemoryChannel memoryChannel = new MemoryChannel();
				ReflectionTestUtils.setField(memoryChannel, "print-stats", true);
				ReflectionTestUtils.setField(memoryChannel, "print-stats-duration-in-seconds", 50000);
				memoryChannel.start();
				ReflectionTestUtils.setField(memoryChannel, "channelStopped", true);
			}
		};
		t.start();
		Thread.sleep(200);
		t.interrupt();
	}

	/**
	 * Assert that stop method sets the "channelStopped" flag to true.
	 */
	@Test
	public void testStop() {
		MemoryChannel memoryChannel = new MemoryChannel();
		memoryChannel.stop();
		Assert.assertEquals(ReflectionTestUtils.getField(memoryChannel, "channelStopped"), true);
	}

	@Test
	public void testGetPutCount() {
		MemoryChannel memoryChannel = new MemoryChannel();
		long putCount = memoryChannel.getPutCount();
		Assert.assertEquals(putCount, 0);
		memoryChannel.put(new ActionEvent());
		putCount = memoryChannel.getPutCount();
		Assert.assertEquals(putCount, 1);
	}

	@Test
	public void testGetTakeCount() {
		MemoryChannel memoryChannel = new MemoryChannel();
		long takeCount = memoryChannel.getTakeCount();
		Assert.assertEquals(takeCount, 0);
		Set<String> consumerNames = new HashSet<>();
		consumerNames.add("c1");
		ReflectionTestUtils.setField(memoryChannel, "consumerNames", consumerNames);
		ReflectionTestUtils.setField(memoryChannel, "channelCapacity", 1000);
		ActionEvent event1 = new ActionEvent();
		event1.setBody("i1".getBytes(Charset.defaultCharset()));
		memoryChannel.put(event1);
		memoryChannel.take("c1");
		takeCount = memoryChannel.getTakeCount();
		Assert.assertEquals(takeCount, 1);

		memoryChannel.printStats();// coverage
	}

	/**
	 * Assert that equals method returns true when compared against the same
	 * object.
	 */
	@Test
	public void testEqualsWithSameObject() {
		MemoryChannel memoryChannel = new MemoryChannel();
		memoryChannel.setName("unit-channel-name");
		memoryChannel.setPropertyMap(new HashMap<String, Object>());
		Assert.assertTrue(memoryChannel.equals(memoryChannel));
	}

	/**
	 * Assert that equals method returns true when both objects have the same
	 * name.
	 */
	@Test
	public void testEqualsAndHashCodeWithSameName() {
		MemoryChannel memoryChannel = new MemoryChannel();
		memoryChannel.setName("unit-channel-name");
		memoryChannel.setPropertyMap(new HashMap<String, Object>());

		MemoryChannel memoryChannel1 = new MemoryChannel();
		memoryChannel1.setName("unit-channel-name");
		memoryChannel1.setPropertyMap(new HashMap<String, Object>());
		Assert.assertTrue(memoryChannel.equals(memoryChannel1));
		Assert.assertTrue(memoryChannel.hashCode() == memoryChannel1.hashCode());
	}

	/**
	 * Assert that equals method returns false when both objects have a
	 * different name.
	 */
	@Test
	public void testEqualsWithDifferentName() {
		MemoryChannel memoryChannel = new MemoryChannel();
		memoryChannel.setName("unit-channel-name-1");
		memoryChannel.setPropertyMap(new HashMap<String, Object>());

		MemoryChannel memoryChannel1 = new MemoryChannel();
		memoryChannel1.setName("unit-channel-name");
		memoryChannel1.setPropertyMap(new HashMap<String, Object>());
		Assert.assertFalse(memoryChannel.equals(memoryChannel1));
	}

	/**
	 * Assert that equals method returns false when compared against null.
	 */
	@Test
	public void testEqualsWithNull() {
		MemoryChannel memoryChannel = new MemoryChannel();
		memoryChannel.setName("unit-channel-name");
		memoryChannel.setPropertyMap(new HashMap<String, Object>());
		Assert.assertFalse(memoryChannel.equals(null));
	}

	/**
	 * Assert that equals method returns false when compared against an object
	 * of different type.
	 */
	@Test
	public void testEqualsWithDifferentObjectType() {
		MemoryChannel memoryChannel = new MemoryChannel();
		memoryChannel.setName("unit-channel-name");
		memoryChannel.setPropertyMap(null);
		Assert.assertFalse(memoryChannel.equals(new Object()));
	}

	/**
	 * Assert that equals method returns false when the calling object's name is
	 * null while other object's name is not null.
	 */
	@Test
	public void testEqualsWithNameAsNullInThis() {
		MemoryChannel memoryChannel = new MemoryChannel();
		memoryChannel.setName(null);
		memoryChannel.setPropertyMap(new HashMap<String, Object>());

		MemoryChannel memoryChannel1 = new MemoryChannel();
		memoryChannel1.setName("unit-channel-name");
		memoryChannel1.setPropertyMap(new HashMap<String, Object>());
		Assert.assertFalse(memoryChannel.equals(memoryChannel1));
	}

	/**
	 * Assert that equals method returns true when both object's name is null.
	 */
	@Test
	public void testEqualsHashCodeWithNameAsNullInBothObjects() {
		MemoryChannel memoryChannel = new MemoryChannel();
		memoryChannel.setName(null);
		memoryChannel.setPropertyMap(new HashMap<String, Object>());

		MemoryChannel memoryChannel1 = new MemoryChannel();
		memoryChannel1.setName(null);
		memoryChannel1.setPropertyMap(new HashMap<String, Object>());
		Assert.assertTrue(memoryChannel.equals(memoryChannel1));
		Assert.assertTrue(memoryChannel.hashCode() == memoryChannel1.hashCode());
	}

	@Test(expectedExceptions = NotImplementedException.class)
	public void testGetTransaction() {
		new MemoryChannel().getTransaction();
	}

	@Test(expectedExceptions = NotImplementedException.class)
	public void testGetLifecycleState() {
		new MemoryChannel().getLifecycleState();
	}

	@Test
	public void testPut() throws InterruptedException {
		final MemoryChannel memoryChannel = new MemoryChannel();

		ReflectionTestUtils.setField(memoryChannel, "channelCapacity", 10);

		Thread t = new Thread() {
			public void run() {
				ActionEvent putEvent = new ActionEvent();
				putEvent.setBody(UUID.randomUUID().toString().getBytes(Charset.defaultCharset()));
				memoryChannel.put(putEvent);
			}
		};

		t.start();
		Thread.sleep(50);
		ReflectionTestUtils.setField(memoryChannel, "channelCapacity", 100);
		t.interrupt();
		t.join();
		Assert.assertEquals(memoryChannel.getPutCount(), 1);
	}
}
