/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.ActionEvent;

public class HandlerContextTest {

	@Test
	public void testGettersAndSetters() {
		HandlerContext handlerContext = HandlerContext.get();
		Assert.assertNull(handlerContext.getJournal("unit-testGettersAndSetters"));
		HandlerJournal dummy = Mockito.mock(HandlerJournal.class);
		handlerContext.setJournal("unit-testGettersAndSetters", dummy);
		Assert.assertSame(handlerContext.getJournal("unit-testGettersAndSetters"), dummy);
		Map<String, HandlerJournal> handlerJournalMap = new HashMap<>();
		HandlerJournal dummy1 = Mockito.mock(HandlerJournal.class);
		handlerJournalMap.put("unit-testGettersAndSetters-1", dummy1);
		handlerContext.setHandlerJournalMap(handlerJournalMap);
		Assert.assertSame(handlerContext.getJournal("unit-testGettersAndSetters-1"), dummy1);
	}

	@Test
	public void testSetEventList() throws Throwable {
		FutureTask<Object> futureTask = new FutureTask<>(new Callable<Object>() {
			@Override
			public Object call() throws Exception {

				HandlerContext handlerContext = HandlerContext.get();
				List<ActionEvent> eventList = new ArrayList<>();
				eventList.add(Mockito.mock(ActionEvent.class));
				eventList.add(Mockito.mock(ActionEvent.class));
				eventList.add(Mockito.mock(ActionEvent.class));
				handlerContext.setEventList(eventList);
				Assert.assertEquals(handlerContext.getEventList().size(), 3);
				return null;
			}
		});
		ExecutorService executorService = Executors.newFixedThreadPool(1);
		executorService.execute(futureTask);
		try {
			futureTask.get();
		} catch (ExecutionException ex) {
			throw ex.getCause();
		}
	}

	/**
	 * Assert that an event can be added conveniently to a newly created
	 * context.
	 * 
	 * @throws Throwable
	 */
	@Test
	public void testCreateSingleItemEventList() throws Throwable {
		FutureTask<Object> futureTask = new FutureTask<>(new Callable<Object>() {
			@Override
			public Object call() throws Exception {

				HandlerContext handlerContext = HandlerContext.get();
				handlerContext.createSingleItemEventList(Mockito.mock(ActionEvent.class));
				Assert.assertEquals(handlerContext.getEventList().size(), 1);
				return null;
			}
		});
		ExecutorService executorService = Executors.newFixedThreadPool(1);
		executorService.execute(futureTask);
		try {
			futureTask.get();
		} catch (ExecutionException ex) {
			throw ex.getCause();
		}
	}

	/**
	 * Assert that clearJournal method sets the journal to null for a given
	 * handler id.
	 */
	@Test
	public void testClearJournal() {
		HandlerContext handlerContext = HandlerContext.get();
		String handlerId = "unit-testClearJournal";

		handlerContext.setJournal(handlerId, Mockito.mock(HandlerJournal.class));
		Assert.assertNotNull(handlerContext.getJournal(handlerId));
		handlerContext.clearJournal(handlerId);
		Assert.assertNull(handlerContext.getJournal(handlerId));
	}
}
