/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.ChannelException;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.AdaptorContextImpl;
import io.bigdime.core.DataChannel;
import io.bigdime.core.HandlerException;

public class MemoryChannelInputHandlerTest {

	/**
	 * Assert that build does not throw any exception if the properties are set.
	 * 
	 * @throws AdaptorConfigurationException
	 */
	@Test
	public void testBuild() throws AdaptorConfigurationException {
		MemoryChannelInputHandler memoryChannelInputHandler = new MemoryChannelInputHandler();
		AdaptorContextImpl.getInstance().setChannels(new ArrayList<DataChannel>());
		Map<String, Object> properties = new HashMap<>();
		memoryChannelInputHandler.setPropertyMap(properties);
		memoryChannelInputHandler.build();
	}

	/**
	 * Assert that memoryChannelInputHandler.process method returns an event if
	 * one is available on the channel. Verify that invoking
	 * memoryChannelInputHandler.process(ActionEvent)} invokes take(String)
	 * method on channel exactly once.
	 * 
	 * @throws HandlerException
	 */
	@Test
	public void testProcess() throws HandlerException {
		MemoryChannelInputHandler memoryChannelInputHandler = new MemoryChannelInputHandler();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		ActionEvent mockEvent = Mockito.mock(ActionEvent.class);

		DataChannel inputChannel = Mockito.mock(DataChannel.class);
		Mockito.when(inputChannel.take(Mockito.any(String.class))).thenReturn(mockEvent);
		ReflectionTestUtils.setField(memoryChannelInputHandler, "inputChannel", inputChannel);

		HandlerContext.get().createSingleItemEventList(mockActionEvent);
		Status status = memoryChannelInputHandler.process();
		Assert.assertSame(status, Status.READY);
		Mockito.verify(inputChannel, Mockito.times(1)).take(Mockito.anyString(), Mockito.anyInt());
	}

	/**
	 * Assert that memoryChannelInputHandler.process method returns an
	 * actionEvent with status as Status.BACKOFF if the channel's take method
	 * throws a ChannelException. Verify that invoking
	 * memoryChannelInputHandler.process(ActionEvent)} invokes take(String)
	 * method on channel exactly once.
	 * 
	 * 
	 * @throws HandlerException
	 */
	@Test
	public void testProcessWithException() throws HandlerException {
		MemoryChannelInputHandler memoryChannelInputHandler = new MemoryChannelInputHandler();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);

		DataChannel inputChannel = Mockito.mock(DataChannel.class);
		Mockito.when(inputChannel.take(Mockito.any(String.class), Mockito.anyInt()))
				.thenThrow(Mockito.mock(ChannelException.class, ""));
		ReflectionTestUtils.setField(memoryChannelInputHandler, "inputChannel", inputChannel);
		HandlerContext.get().createSingleItemEventList(mockActionEvent);
		Status status = memoryChannelInputHandler.process();
		Assert.assertSame(status, Status.BACKOFF);
		Mockito.verify(inputChannel, Mockito.times(1)).take(Mockito.anyString(), Mockito.anyInt());
	}
}
