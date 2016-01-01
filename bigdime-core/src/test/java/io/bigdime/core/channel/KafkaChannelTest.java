/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.channel;

import org.apache.commons.lang.NotImplementedException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import io.bigdime.core.ActionEvent;

public class KafkaChannelTest {

	@Test(expectedExceptions = NotImplementedException.class)
	public void testBuild() {
		new KafkaChannel().build();
	}

	@Test(expectedExceptions = NotImplementedException.class)
	public void testPut() {
		new KafkaChannel().put(Mockito.mock(ActionEvent.class));
	}

	@Test(expectedExceptions = NotImplementedException.class)
	public void testTakeOneEvent() {
		KafkaChannel kafkaChannel = new KafkaChannel();
		kafkaChannel.take(1);
	}

	@Test(expectedExceptions = NotImplementedException.class)
	public void testTakeWithNoArguments() {
		new KafkaChannel().take();
	}

	@Test(expectedExceptions = NotImplementedException.class)
	public void testRegisterConsumer() {
		new KafkaChannel().registerConsumer("unit");
	}

	@Test(expectedExceptions = NotImplementedException.class)
	public void testTakeWithConsumer() {
		new KafkaChannel().take("unit-consumer");
	}

	@Test(expectedExceptions = NotImplementedException.class)
	public void testTakeWithConsumerAndSize() {
		new KafkaChannel().take("unit-consumer", 1);
	}

}
