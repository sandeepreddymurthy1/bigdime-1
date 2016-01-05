/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.line;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.DataChannel;
import io.bigdime.core.HandlerException;
import io.bigdime.core.handler.HandlerContext;

public class LineHandlerTest {

	@Test
	public void testBuild() throws AdaptorConfigurationException {
		LineHandler lineHandler = new LineHandler();
		Map<String, Object> propertyMap = new HashMap<>();
		lineHandler.setPropertyMap(propertyMap);
		lineHandler.build();
		// no exception thrown
	}

	/**
	 * <p>
	 * <ul>
	 * <li>Test with one event in handler context and a new line char in the
	 * body.
	 * <li>It should return READY and set the remaining chars in journal's
	 * leftover event.
	 * <li>The resultant HandlerContext should have data including new line
	 * char.
	 * </ul>
	 * 
	 * @throws HandlerException
	 */
	@Test
	public void testProcessWithOneEventWithNewLineChar() throws HandlerException {
		LineHandler lineHandler = new LineHandler();
		ActionEvent actionEvent1 = new ActionEvent();
		actionEvent1.setBody("testProcessWithOneEventWithNewLineChar-\n with new line char".getBytes());
		HandlerContext.get().createSingleItemEventList(actionEvent1);

		DataChannel channel = Mockito.mock(DataChannel.class);
		ReflectionTestUtils.setField(lineHandler, "outputChannel", channel);
		Mockito.doNothing().when(channel).put(Mockito.any(Event.class));

		Status status = lineHandler.process();
		Assert.assertEquals(status, Status.READY);
		LineHandlerJournal journal = (LineHandlerJournal) (HandlerContext.get().getJournal(lineHandler.getId()));
		Assert.assertEquals(new String(HandlerContext.get().getEventList().get(0).getBody(), Charset.defaultCharset()),
				"testProcessWithOneEventWithNewLineChar-\n");
		Assert.assertEquals(new String(journal.getLeftoverEvent().getBody()), " with new line char");
		Mockito.verify(channel, Mockito.times(1)).put(Mockito.any(Event.class));
	}

	/**
	 * <p>
	 * <ul>
	 * <li>Test with one event in handler context and NO new line char in the
	 * body.
	 * <li>It should return READY and set the whole body in journal's leftover
	 * event.
	 * <li>The resultant HandlerContext should be null;
	 * </ul>
	 * 
	 * @throws HandlerException
	 */

	@Test
	public void testProcessWithOneEventWithNoNewLineChar() throws HandlerException {
		LineHandler lineHandler = new LineHandler();
		ActionEvent actionEvent1 = new ActionEvent();
		actionEvent1.setBody("testProcessWithOneEventWithNewLineChar-with no new line char".getBytes());
		HandlerContext.get().createSingleItemEventList(actionEvent1);
		Status status = lineHandler.process();
		Assert.assertEquals(status, Status.READY);
		LineHandlerJournal journal = (LineHandlerJournal) (HandlerContext.get().getJournal(lineHandler.getId()));
		Assert.assertEquals(new String(journal.getLeftoverEvent().getBody()),
				"testProcessWithOneEventWithNewLineChar-with no new line char");
		Assert.assertNull(journal.getEventList());
		Assert.assertNull(HandlerContext.get().getEventList());
	}

	/**
	 * <p>
	 * <ul>
	 * <li>Test with multiple events in handler context and a new line char in
	 * the body of the first event.
	 * <li>It should return CALLBACK and set the remaining chars in journal's
	 * leftover event.
	 * <li>The resultant HandlerContext should have one event and journal should
	 * have rest of the events in the list.
	 * </ul>
	 * 
	 * @throws HandlerException
	 */
	@Test
	public void testProcessWithMultipleEventsWithNewLineCharInFirstEvent() throws HandlerException {
		LineHandler lineHandler = new LineHandler();
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.setBody("testProcessWithMultipleEventsWithNewLineCharInFirstEvent\n-event-0".getBytes());
		HandlerContext.get().createSingleItemEventList(actionEvent);

		for (int i = 1; i < 10; i++) {
			actionEvent = new ActionEvent();
			actionEvent.setBody(("testProcessWithMultipleEventsWithNewLineCharInFirstEvent-event-" + i).getBytes());
			HandlerContext.get().getEventList().add(actionEvent);
		}

		Status status = lineHandler.process();
		Assert.assertEquals(status, Status.CALLBACK);
		LineHandlerJournal journal = (LineHandlerJournal) (HandlerContext.get().getJournal(lineHandler.getId()));
		Assert.assertEquals(new String(journal.getLeftoverEvent().getBody()), "-event-0");
		Assert.assertEquals(journal.getEventList().size(), 9);
		Assert.assertEquals(new String(HandlerContext.get().getEventList().get(0).getBody(), Charset.defaultCharset()),
				"testProcessWithMultipleEventsWithNewLineCharInFirstEvent\n");
	}

	/**
	 * <p>
	 * <ul>
	 * <li>Test with multiple events in handler context and a new line char in
	 * the body of the last event.
	 * <li>It should return READY and set the remaining chars in journal's
	 * leftover event.
	 * <li>The resultant HandlerContext should have one event and journal should
	 * not have any events.
	 * </ul>
	 * 
	 * @throws HandlerException
	 */
	@Test
	public void testProcessWithMultipleEventsWithNewLineCharInLastEvent() throws HandlerException {
		LineHandler lineHandler = new LineHandler();
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.setBody("event-0".getBytes());
		HandlerContext.get().createSingleItemEventList(actionEvent);

		for (int i = 1; i < 10; i++) {
			actionEvent = new ActionEvent();
			actionEvent.setBody(("event-" + i).getBytes());
			HandlerContext.get().getEventList().add(actionEvent);
		}
		actionEvent = new ActionEvent();
		actionEvent.setBody(("event\n-10").getBytes());
		HandlerContext.get().getEventList().add(actionEvent);

		Status status = lineHandler.process();
		Assert.assertEquals(status, Status.READY);
		LineHandlerJournal journal = (LineHandlerJournal) (HandlerContext.get().getJournal(lineHandler.getId()));
		Assert.assertEquals(new String(journal.getLeftoverEvent().getBody()), "-10");
		Assert.assertNull(journal.getEventList());
		Assert.assertEquals(HandlerContext.get().getEventList().size(), 1);
		Assert.assertTrue(new String(HandlerContext.get().getEventList().get(0).getBody(), Charset.defaultCharset())
				.startsWith("event-0event-1event-2"));
		Assert.assertTrue(new String(HandlerContext.get().getEventList().get(0).getBody(), Charset.defaultCharset())
				.endsWith("event-9event\n"));
	}

	/**
	 * <p>
	 * <ul>
	 * <li>Test with multiple events in handler context and no new line char in
	 * the body of any event.
	 * <li>It should return READY and set all chars in journal's leftover event.
	 * <li>The resultant HandlerContext and journal should have null event list.
	 * </ul>
	 * 
	 * @throws HandlerException
	 */
	@Test
	public void testProcessWithMultipleEventsWithNoNewLineChar() throws HandlerException {
		LineHandler lineHandler = new LineHandler();
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.setBody("event-0".getBytes());
		HandlerContext.get().createSingleItemEventList(actionEvent);

		for (int i = 1; i < 10; i++) {
			actionEvent = new ActionEvent();
			actionEvent.setBody(("event-" + i).getBytes());
			HandlerContext.get().getEventList().add(actionEvent);
		}
		actionEvent = new ActionEvent();
		actionEvent.setBody(("event-10").getBytes());
		HandlerContext.get().getEventList().add(actionEvent);

		Status status = lineHandler.process();
		Assert.assertEquals(status, Status.READY);
		LineHandlerJournal journal = (LineHandlerJournal) (HandlerContext.get().getJournal(lineHandler.getId()));
		Assert.assertTrue(new String(journal.getLeftoverEvent().getBody(), Charset.defaultCharset())
				.startsWith("event-0event-1event-2"));
		Assert.assertTrue(new String(journal.getLeftoverEvent().getBody(), Charset.defaultCharset())
				.endsWith("event-8event-9event-10"));
		Assert.assertNull(journal.getEventList());
		Assert.assertNull(HandlerContext.get().getEventList());
	}

	/**
	 * <p>
	 * <ul>
	 * <li>Test with multiple events, with new line char in first event and a
	 * leftover event in journal.
	 * 
	 * <li>It should return CALLBACK and set the remaining chars in journal's
	 * leftover event.
	 * <li>The resultant HandlerContext should have one event and journal should
	 * have rest of the events in the list.
	 * 
	 * @throws HandlerException
	 */
	@Test
	public void testProcessWithMultipleEventsAndLeftoverEventInJournal() throws HandlerException {

		LineHandler lineHandler = new LineHandler();
		LineHandlerJournal journal = new LineHandlerJournal();
		HandlerContext.get().setJournal(lineHandler.getId(), journal);
		ActionEvent leftoverEvent = new ActionEvent();
		leftoverEvent.setBody("leftover-event-0".getBytes());
		journal.setLeftoverEvent(leftoverEvent);

		ActionEvent actionEvent = new ActionEvent();
		actionEvent.setBody("testProcessWithMultipleEventsAndLeftoverEventInJournal\n-event-0".getBytes());
		journal.setEventList(new ArrayList<ActionEvent>());
		journal.getEventList().add(actionEvent);
		for (int i = 1; i < 10; i++) {
			actionEvent = new ActionEvent();
			actionEvent.setBody(("testProcessWithMultipleEventsAndLeftoverEventInJournal-event-" + i).getBytes());
			journal.getEventList().add(actionEvent);
		}

		Status status = lineHandler.process();
		Assert.assertEquals(status, Status.CALLBACK);
		Assert.assertEquals(new String(journal.getLeftoverEvent().getBody()), "-event-0");
		Assert.assertEquals(journal.getEventList().size(), 9);
		Assert.assertTrue(new String(HandlerContext.get().getEventList().get(0).getBody(), Charset.defaultCharset())
				.equals("leftover-event-0testProcessWithMultipleEventsAndLeftoverEventInJournal\n"));
	}

	/**
	 * <p>
	 * <ul>
	 * <li>Test with multiple events, with new line char in first event and no
	 * leftover event in journal.
	 * 
	 * <li>It should return CALLBACK and set the remaining chars in journal's
	 * leftover event.
	 * <li>The resultant HandlerContext should have one event and journal should
	 * have rest of the events in the list.
	 * 
	 * @throws HandlerException
	 */
	@Test
	public void testProcessWithMultipleEventsAndNoLeftoverEventInJournal() throws HandlerException {
		LineHandler lineHandler = new LineHandler();
		LineHandlerJournal journal = new LineHandlerJournal();
		HandlerContext.get().setJournal(lineHandler.getId(), journal);

		ActionEvent actionEvent = new ActionEvent();
		actionEvent.setBody("testProcessWithMultipleEventsAndNoLeftoverEventInJournal\n-event-0".getBytes());
		journal.setEventList(new ArrayList<ActionEvent>());
		journal.getEventList().add(actionEvent);
		for (int i = 1; i < 10; i++) {
			actionEvent = new ActionEvent();
			actionEvent.setBody(("testProcessWithMultipleEventsAndNoLeftoverEventInJournal-event-" + i).getBytes());
			journal.getEventList().add(actionEvent);
		}

		Status status = lineHandler.process();
		Assert.assertEquals(status, Status.CALLBACK);
		Assert.assertEquals(new String(journal.getLeftoverEvent().getBody()), "-event-0");
		Assert.assertEquals(journal.getEventList().size(), 9);
		Assert.assertEquals(HandlerContext.get().getEventList().size(), 1);
		Assert.assertTrue(new String(HandlerContext.get().getEventList().get(0).getBody(), Charset.defaultCharset())
				.equals("testProcessWithMultipleEventsAndNoLeftoverEventInJournal\n"));
	}

	/**
	 * <p>
	 * <ul>
	 * <li>Test with a leftover event in journal, and few events in context with
	 * first event ending with a new line char.
	 * 
	 * <li>It should return CALLBACK and set an empty body in journal's leftover
	 * event.
	 * <li>The resultant HandlerContext should have one event and journal should
	 * have rest of the events in the list.
	 * 
	 * @throws HandlerException
	 */
	@Test
	public void testProcessWithLeftoverEventInJournal() throws HandlerException {
		LineHandler lineHandler = new LineHandler();
		LineHandlerJournal journal = new LineHandlerJournal();
		HandlerContext.get().setJournal(lineHandler.getId(), journal);
		ActionEvent leftoverEvent = new ActionEvent();
		leftoverEvent.setBody("leftover-event-0".getBytes());
		journal.setLeftoverEvent(leftoverEvent);

		ActionEvent actionEvent = new ActionEvent();
		actionEvent.setBody("testProcessWithLeftoverEventInJournal\n".getBytes());
		// actionEvent.setBody("testProcessWithLeftoverEventInJournal\n-event-0".getBytes());
		HandlerContext.get().createSingleItemEventList(actionEvent);
		for (int i = 1; i < 10; i++) {
			actionEvent = new ActionEvent();
			actionEvent.setBody(("testProcessWithLeftoverEventInJournal-event-" + i).getBytes());
			HandlerContext.get().getEventList().add(actionEvent);
		}

		Status status = lineHandler.process();
		Assert.assertEquals(status, Status.CALLBACK);
		Assert.assertTrue(new String(journal.getLeftoverEvent().getBody(), Charset.defaultCharset()).isEmpty(),
				"if an event's body ends with a new line char, leftover event must have empty body");
		Assert.assertEquals(journal.getEventList().size(), 9);
		Assert.assertEquals(HandlerContext.get().getEventList().size(), 1);
		Assert.assertTrue(new String(HandlerContext.get().getEventList().get(0).getBody(), Charset.defaultCharset())
				.equals("leftover-event-0testProcessWithLeftoverEventInJournal\n"));
	}

}
