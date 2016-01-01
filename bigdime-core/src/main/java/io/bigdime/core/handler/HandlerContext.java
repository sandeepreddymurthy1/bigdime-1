/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.bigdime.core.ActionEvent;

public final class HandlerContext {
	/**
	 * Only one HandlerContext object is needed per thread.
	 */
	public static final ThreadLocal<HandlerContext> handlerContext = new ThreadLocal<HandlerContext>() {
		@Override
		protected HandlerContext initialValue() {
			return new HandlerContext();
		}
	};

	private HandlerContext() {

	}

	public static final HandlerContext get() {
		return handlerContext.get();
	}

	private Context ctx = null;

	private Context getContext() {
		final HandlerContext context = get();
		if (context.ctx == null) {
			context.ctx = new Context();
		}
		return context.ctx;

	}

	private static final class Context {

		private List<ActionEvent> eventList = new ArrayList<>();
		private Map<String, HandlerJournal> handlerJournalMap = new HashMap<>();

		/**
		 * totalSize=0 totalRead=0 thisIterationNumber totalIterationsExpected
		 */

		public Map<String, HandlerJournal> getHandlerJournalMap() {
			return handlerJournalMap;
		}

		public void setHandlerJournalMap(Map<String, HandlerJournal> handlerJournalMap) {
			this.handlerJournalMap = handlerJournalMap;
		}

		public List<ActionEvent> getEventList() {
			return eventList;
		}

		public void setEventList(List<ActionEvent> eventList) {
			this.eventList = eventList;
		}
	}

	public Object getJournal(String handlerId) {
		return getContext().getHandlerJournalMap().get(handlerId);
	}

	public void clearJournal(String handlerId) {
		setJournal(handlerId, null);
	}

	public void setJournal(String handlerId, HandlerJournal handlerJournal) {
		getContext().getHandlerJournalMap().put(handlerId, handlerJournal);
	}

	public void setHandlerJournalMap(Map<String, HandlerJournal> handlerJournalMap) {
		getContext().setHandlerJournalMap(handlerJournalMap);
	}

	public List<ActionEvent> getEventList() {
		return getContext().getEventList();
	}

	public void setEventList(List<ActionEvent> eventList) {
		getContext().setEventList(eventList);
	}

	public void createSingleItemEventList(ActionEvent actionEvent) {
		List<ActionEvent> eventList = new ArrayList<>();
		eventList.add(actionEvent);
		getContext().setEventList(eventList);
	}

	// public void reset() {
	// // getContext().setDoneProcessingMap(new HashMap<String, Boolean>());
	// // getContext().setHandlerJournalMap(new HashMap<String, Map<String,
	// // Object>>());
	// }

	// @Override
	// public String toString() {
	// return getContext().toString();
	// }

}
