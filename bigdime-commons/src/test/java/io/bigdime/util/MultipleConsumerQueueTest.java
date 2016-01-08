/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.util;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MultipleConsumerQueueTest {

	/**
	 * Assert that {@link MultipleConsumerQueue#registerConsumer(String)} method
	 * adds consumers to the consumerNames set.
	 */
	@Test
	public void testRegister() {
		MultipleConsumerQueue<Object> multipleConsumerQueue = new MultipleConsumerQueue<>();
		String consumerOne = "c-1";
		String consumerTwo = "c-2";

		multipleConsumerQueue.registerConsumer(consumerOne);
		multipleConsumerQueue.registerConsumer(consumerTwo);

		Assert.assertEquals(multipleConsumerQueue.getConsumerNames().size(), 2);
		Assert.assertTrue(multipleConsumerQueue.getConsumerNames().contains(consumerOne));
		Assert.assertTrue(multipleConsumerQueue.getConsumerNames().contains(consumerTwo));
	}

	/**
	 * Test to assert that when more than one items are present on the queue,
	 * multiple consecutive invocations of {@link MultipleConsumerQueue#peek()}
	 * method returns the same item.
	 */
	@Test
	public void testPeek() {
		MultipleConsumerQueue<Object> multipleConsumerQueue = new MultipleConsumerQueue<>();

		Object[] Objects = new Object[10];
		for (int i = 0; i < 10; i++) {
			Objects[i] = Mockito.mock(Object.class);
			Mockito.when(Objects[i].toString()).thenReturn("event = " + i);
			boolean add = multipleConsumerQueue.offer(Objects[i]);
			Assert.assertTrue(add);
		}
		Object actualEvents = multipleConsumerQueue.peek();
		Assert.assertEquals(actualEvents, Objects[0]);
		actualEvents = multipleConsumerQueue.peek();
		// peek again and
		Assert.assertEquals(actualEvents, Objects[0]);
	}

	/**
	 * Assert that invoking {@link MultipleConsumerQueue#peek()} method on the
	 * queue that has registered consumers, throws an
	 * UnsupportedOperationException.
	 */
	@Test(expectedExceptions = UnsupportedOperationException.class)
	public void testPeekWithConsumers() {
		MultipleConsumerQueue<Object> multipleConsumerQueue = new MultipleConsumerQueue<>();
		multipleConsumerQueue.registerConsumer("unit-consumer-1");
		multipleConsumerQueue.peek();
	}

	/**
	 * Assert that a {@link MultipleConsumerQueue#peek(String)} method returns
	 * null if there are no items available on the queue.
	 */
	@Test
	public void testPeekWithNoItemsOnQueue() {
		MultipleConsumerQueue<Object> multipleConsumerQueue = new MultipleConsumerQueue<>();
		multipleConsumerQueue.registerConsumer("unit-consumer-1");
		Object actualEvent = multipleConsumerQueue.peek("unit-consumer-1");
		// peek again and
		Assert.assertNull(actualEvent);
	}

	/**
	 * Test to assert that when more than one items are present on the queue,
	 * {@link MultipleConsumerQueue#poll())} method returns the items in the
	 * order they were inserted in.
	 */
	@Test
	public void testPoll() {
		MultipleConsumerQueue<Object> multipleConsumerQueue = new MultipleConsumerQueue<>();

		Object[] Objects = new Object[10];
		for (int i = 0; i < 10; i++) {
			Objects[i] = Mockito.mock(Object.class);
			Mockito.when(Objects[i].toString()).thenReturn("event = " + i);
			boolean add = multipleConsumerQueue.offer(Objects[i]);
			Assert.assertTrue(add);
		}
		Object actualEvent = multipleConsumerQueue.poll();
		Assert.assertEquals(actualEvent, Objects[0]);
		actualEvent = multipleConsumerQueue.poll();
		Assert.assertEquals(actualEvent, Objects[1]);
	}

	/**
	 * Assert that invoking {@link MultipleConsumerQueue#poll()} method on the
	 * queue that has registered consumers, throws an
	 * UnsupportedOperationException.
	 */
	@Test(expectedExceptions = UnsupportedOperationException.class)
	public void testPollWithConsumers() {
		MultipleConsumerQueue<Object> multipleConsumerQueue = new MultipleConsumerQueue<>();
		multipleConsumerQueue.registerConsumer("unit-consumer-1");
		multipleConsumerQueue.poll();
	}

	/**
	 * Test to assert that when more than one items are present on the queue,
	 * {@link MultipleConsumerQueue#poll(int))} method returns the items in the
	 * order they were inserted in.
	 */
	@Test
	public void testPollInt() {
		MultipleConsumerQueue<Object> multipleConsumerQueue = new MultipleConsumerQueue<>();

		Object[] Objects = new Object[10];
		for (int i = 0; i < 10; i++) {
			Objects[i] = Mockito.mock(Object.class);
			Mockito.when(Objects[i].toString()).thenReturn("event = " + i);
			boolean add = multipleConsumerQueue.offer(Objects[i]);
			Assert.assertTrue(add);
		}
		List<Object> actualEvents = multipleConsumerQueue.poll(11);
		Assert.assertEquals(actualEvents.get(0), Objects[0]);
		Assert.assertEquals(actualEvents.get(1), Objects[1]);
	}

	/**
	 * Assert that a {@link MultipleConsumerQueue#poll(int)} method returns null
	 * if there are no items available on the queue.
	 */
	@Test
	public void testPollIntWithNoItemsOnQueue() {
		MultipleConsumerQueue<Object> multipleConsumerQueue = new MultipleConsumerQueue<>();
		List<Object> actualEvents = multipleConsumerQueue.poll(1);
		Assert.assertNull(actualEvents);
	}

	/**
	 * Assert that invoking {@link MultipleConsumerQueue#poll(int)} method on
	 * the queue that has registered consumers, throws an
	 * UnsupportedOperationException.
	 */

	@Test(expectedExceptions = UnsupportedOperationException.class)
	public void testPollIntWithConsumers() {
		MultipleConsumerQueue<Object> multipleConsumerQueue = new MultipleConsumerQueue<>();
		multipleConsumerQueue.registerConsumer("unit-consumer-1");
		multipleConsumerQueue.poll(1);
	}

	/**
	 * Assert that invoking {@link MultipleConsumerQueue#size()} method on the
	 * queue that has registered consumers, throws an
	 * UnsupportedOperationException.
	 */
	@Test(expectedExceptions = UnsupportedOperationException.class)
	public void testSizeWithConsumersForUnsupportedOperationException() {
		MultipleConsumerQueue<Object> multipleConsumerQueue = new MultipleConsumerQueue<>();
		multipleConsumerQueue.registerConsumer("unit-consumer-1");
		multipleConsumerQueue.size();
	}

	/**
	 * Assert that a {@link MultipleConsumerQueue#size(String)} method returns
	 * actual queue size.
	 */
	@Test
	public void testSizeWithConsumers() {
		MultipleConsumerQueue<Object> multipleConsumerQueue = new MultipleConsumerQueue<>();
		multipleConsumerQueue.registerConsumer("unit-consumer-1");

		Object[] Objects = new Object[10];
		for (int i = 0; i < 10; i++) {
			Objects[i] = Mockito.mock(Object.class);
			Mockito.when(Objects[i].toString()).thenReturn("event = " + i);
			multipleConsumerQueue.offer(Objects[i]);
		}
		int size = multipleConsumerQueue.size("unit-consumer-1");
		Assert.assertEquals(size, 10);
	}

	/**
	 * Assert that a {@link MultipleConsumerQueue#size()} method returns actual
	 * queue size.
	 */
	@Test
	public void testSizeWithDefaultConsumer() {
		MultipleConsumerQueue<Object> multipleConsumerQueue = new MultipleConsumerQueue<>();
		Object[] Objects = new Object[10];
		for (int i = 0; i < 10; i++) {
			Objects[i] = Mockito.mock(Object.class);
			Mockito.when(Objects[i].toString()).thenReturn("event = " + i);
			multipleConsumerQueue.offer(Objects[i]);
		}
		int size = multipleConsumerQueue.size();
		Assert.assertEquals(size, 10);
	}

	/**
	 * Assert that {@link MultipleConsumerQueue#unregisterConsumer(String)}
	 * method returns true.
	 */
	@Test
	public void testUnregister() {
		MultipleConsumerQueue<Object> multipleConsumerQueue = new MultipleConsumerQueue<>();
		String consumerOne = "c-1";
		String consumerTwo = "c-2";
		multipleConsumerQueue.registerConsumer(consumerOne);
		multipleConsumerQueue.registerConsumer(consumerTwo);
		Object[] Objects = new Object[10];
		for (int i = 0; i < 10; i++) {
			Objects[i] = Mockito.mock(Object.class);
			Mockito.when(Objects[i].toString()).thenReturn("event = " + i);
			boolean add = multipleConsumerQueue.offer(Objects[i]);
			Assert.assertTrue(add);
		}
		Object peekedEventOne = multipleConsumerQueue.peek(consumerOne);
		Assert.assertSame(peekedEventOne, Objects[0]);

		Object polledEventOne = multipleConsumerQueue.poll(consumerOne);
		Assert.assertSame(polledEventOne, Objects[0]);

		Assert.assertEquals(ReflectionTestUtils.getField(multipleConsumerQueue, "removedCount"), 0);

		Object peekedEventTwo = multipleConsumerQueue.peek(consumerTwo);
		Assert.assertSame(peekedEventTwo, Objects[0]);

		Object polledEventTwo = multipleConsumerQueue.poll(consumerTwo);
		Assert.assertSame(polledEventTwo, Objects[0]);
		Assert.assertEquals(ReflectionTestUtils.getField(multipleConsumerQueue, "removedCount"), 1);

		multipleConsumerQueue.poll(consumerOne);
		multipleConsumerQueue.poll(consumerOne);
		multipleConsumerQueue.poll(consumerOne);
		Assert.assertEquals(ReflectionTestUtils.getField(multipleConsumerQueue, "removedCount"), 1);

		peekedEventTwo = multipleConsumerQueue.peek(consumerTwo);
		Assert.assertSame(peekedEventTwo, Objects[1]);
		polledEventTwo = multipleConsumerQueue.poll(consumerTwo);
		Assert.assertSame(polledEventTwo, Objects[1]);
		Assert.assertEquals(ReflectionTestUtils.getField(multipleConsumerQueue, "removedCount"), 2);

		peekedEventOne = multipleConsumerQueue.peek(consumerOne);
		Assert.assertSame(peekedEventOne, Objects[4]);
		polledEventOne = multipleConsumerQueue.poll(consumerOne);
		Assert.assertSame(polledEventOne, Objects[4]);
		Assert.assertEquals(ReflectionTestUtils.getField(multipleConsumerQueue, "removedCount"), 2);

		boolean unregistered = multipleConsumerQueue.unregisterConsumer(consumerOne);
		Assert.assertTrue(unregistered);

		peekedEventTwo = multipleConsumerQueue.peek(consumerTwo);
		Assert.assertSame(peekedEventTwo, Objects[2]);
		polledEventTwo = multipleConsumerQueue.poll(consumerTwo);
		Assert.assertSame(polledEventTwo, Objects[2]);
		Assert.assertEquals(ReflectionTestUtils.getField(multipleConsumerQueue, "removedCount"), 3);

	}

	/**
	 * @formatter:off
	 * A queue with 2 consumers.
	 * Add a message to the queue.
	 * consumerOne polls a message.
	 * unregister consumerTwo.
	 * 
	 * @formatter:on
	 * 
	 */
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testUnregister2() {
		MultipleConsumerQueue<Object> multipleConsumerQueue = new MultipleConsumerQueue<>();
		String consumerOne = "c-1";
		String consumerTwo = "c-2";
		multipleConsumerQueue.registerConsumer(consumerOne);
		multipleConsumerQueue.registerConsumer(consumerTwo);
		Object Object = Mockito.mock(Object.class);
		Mockito.when(Object.toString()).thenReturn("event = 0");
		multipleConsumerQueue.offer(Object);

		Object polledEventOne = multipleConsumerQueue.poll(consumerOne);
		Assert.assertSame(polledEventOne, Object);
		Assert.assertEquals(ReflectionTestUtils.getField(multipleConsumerQueue, "removedCount"), 0);
		multipleConsumerQueue.unregisterConsumer(consumerTwo);

		Assert.assertNull(multipleConsumerQueue.peek(consumerOne));
		Assert.assertEquals(multipleConsumerQueue.size(consumerOne), 0);
		multipleConsumerQueue.peek(consumerTwo);
	}

	@Test
	public void testUnregister1() {
		MultipleConsumerQueue<Object> multipleConsumerQueue = new MultipleConsumerQueue<>();
		String consumerOne = "c-1";
		String consumerTwo = "c-2";
		String consumerThree = "c-3";
		String consumerFour = "c-4";
		multipleConsumerQueue.registerConsumer(consumerOne);
		multipleConsumerQueue.registerConsumer(consumerTwo);
		multipleConsumerQueue.registerConsumer(consumerThree);
		multipleConsumerQueue.registerConsumer(consumerFour);
		Object[] Objects = new Object[10];
		for (int i = 0; i < 10; i++) {
			Objects[i] = Mockito.mock(Object.class);
			Mockito.when(Objects[i].toString()).thenReturn("event = " + i);
			boolean add = multipleConsumerQueue.offer(Objects[i]);
			Assert.assertTrue(add);
		}
		Object peekedEventOne = multipleConsumerQueue.peek(consumerOne);
		Assert.assertSame(peekedEventOne, Objects[0]);

		Object polledEventOne = multipleConsumerQueue.poll(consumerOne);
		Assert.assertSame(polledEventOne, Objects[0]);

		Assert.assertEquals(ReflectionTestUtils.getField(multipleConsumerQueue, "removedCount"), 0);

		Object peekedEventTwo = multipleConsumerQueue.peek(consumerTwo);
		Assert.assertSame(peekedEventTwo, Objects[0]);

		Object polledEventTwo = multipleConsumerQueue.poll(consumerTwo);
		Assert.assertSame(polledEventTwo, Objects[0]);
		Assert.assertEquals(ReflectionTestUtils.getField(multipleConsumerQueue, "removedCount"), 0);

		multipleConsumerQueue.poll(consumerOne);
		multipleConsumerQueue.poll(consumerOne);
		multipleConsumerQueue.poll(consumerOne);
		multipleConsumerQueue.poll(consumerTwo);
		multipleConsumerQueue.poll(consumerTwo);
		// multipleConsumerQueue.poll(consumerTwo);
		multipleConsumerQueue.poll(consumerThree);
		Assert.assertEquals(ReflectionTestUtils.getField(multipleConsumerQueue, "removedCount"), 0);

		peekedEventTwo = multipleConsumerQueue.peek(consumerTwo);
		Assert.assertSame(peekedEventTwo, Objects[3]);
		polledEventTwo = multipleConsumerQueue.poll(consumerTwo);
		Assert.assertSame(polledEventTwo, Objects[3]);
		Assert.assertEquals(ReflectionTestUtils.getField(multipleConsumerQueue, "removedCount"), 0);

		peekedEventOne = multipleConsumerQueue.peek(consumerOne);
		Assert.assertSame(peekedEventOne, Objects[4]);
		polledEventOne = multipleConsumerQueue.poll(consumerOne);
		Assert.assertSame(polledEventOne, Objects[4]);
		Assert.assertEquals(ReflectionTestUtils.getField(multipleConsumerQueue, "removedCount"), 0);

		boolean unregistered = multipleConsumerQueue.unregisterConsumer(consumerThree);
		Assert.assertTrue(unregistered);

		peekedEventTwo = multipleConsumerQueue.peek(consumerTwo);
		Assert.assertSame(peekedEventTwo, Objects[4]);
		polledEventTwo = multipleConsumerQueue.poll(consumerTwo);
		Assert.assertSame(polledEventTwo, Objects[4]);
		Assert.assertEquals(ReflectionTestUtils.getField(multipleConsumerQueue, "removedCount"), 0);

		polledEventOne = multipleConsumerQueue.poll(consumerOne);
		Assert.assertSame(polledEventOne, Objects[5]);
		Assert.assertEquals(ReflectionTestUtils.getField(multipleConsumerQueue, "removedCount"), 0);
		unregistered = multipleConsumerQueue.unregisterConsumer(consumerFour);
		Assert.assertEquals(ReflectionTestUtils.getField(multipleConsumerQueue, "removedCount"), 5);
	}

	@Test
	public void testIteratorNotNull() {
		MultipleConsumerQueue<Object> multipleConsumerQueue = new MultipleConsumerQueue<>();
		Assert.assertNotNull(multipleConsumerQueue.iterator());
	}

	@Test
	public void testIteratorWithItems() {
		MultipleConsumerQueue<Object> multipleConsumerQueue = new MultipleConsumerQueue<>();
		Object[] Objects = new Object[10];
		for (int i = 0; i < 10; i++) {
			Objects[i] = Mockito.mock(Object.class);
			Mockito.when(Objects[i].toString()).thenReturn("event = " + i);
			boolean add = multipleConsumerQueue.offer(Objects[i]);
			Assert.assertTrue(add);
		}
		Iterator<Object> iter = multipleConsumerQueue.iterator();

		for (int i = 0; i < 10; i++) {
			Assert.assertTrue(iter.hasNext());
			Assert.assertSame(iter.next(), Objects[i]);
		}
		Assert.assertFalse(iter.hasNext());
	}

	@Test(expectedExceptions = NoSuchElementException.class)
	public void testIteratorWithItemsWithNoSuchElementException() {
		MultipleConsumerQueue<Object> multipleConsumerQueue = new MultipleConsumerQueue<>();
		Iterator<Object> iter = multipleConsumerQueue.iterator();
		iter.next();
	}

	@Test(expectedExceptions = UnsupportedOperationException.class)
	public void testRemove() {
		new MultipleConsumerQueue<>().iterator().remove();
	}
}