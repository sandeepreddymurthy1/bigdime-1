/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.channel;

import java.util.ArrayList;
import java.util.List;

import org.apache.flume.ChannelSelector;
import org.apache.flume.Event;

import io.bigdime.core.ActionEvent;

public class DataChannelProcessor extends org.apache.flume.channel.ChannelProcessor {
//	private static final ThreadLocal<HandlerContext<FlumeSourceContext>> context = new ThreadLocal<HandlerContext<FlumeSourceContext>>();

	public DataChannelProcessor(ChannelSelector selector) {
		super(selector);
	}

	/**
	 * Put the data in HandlerContext and return from here, skip
	 */
	@Override
	public void processEventBatch(List<Event> events) {
		List<ActionEvent> actionEvents = new ArrayList<>();
		for (Event event : events) {
			ActionEvent actionEvent = new ActionEvent(event);
			actionEvents.add(actionEvent);
//			context.get().get().setActionEvents(actionEvents);
		}
	}

	/**
	 * Skip interceptor processing, put the event data in HandlerContext and
	 * return from here.
	 * 
	 * @param event
	 */
	@Override
	public void processEvent(Event event) {
//		ActionEvent actionEvent = new ActionEvent(event);
//		context.get().get().setActionEvent(actionEvent);

	}

	private void skipInterceptorsAndPopulateHandlerContext() {

	}

}
