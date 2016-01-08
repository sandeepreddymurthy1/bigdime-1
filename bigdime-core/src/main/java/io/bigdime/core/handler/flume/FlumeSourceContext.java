/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler.flume;

import io.bigdime.core.ActionEvent;

import java.util.List;

public class FlumeSourceContext {
	private ActionEvent actionEvent;
	private List<ActionEvent> actionEvents;

	public ActionEvent getActionEvent() {
		return actionEvent;
	}

	public void setActionEvent(ActionEvent actionEvent) {
		this.actionEvent = actionEvent;
	}

	public List<ActionEvent> getActionEvents() {
		return actionEvents;
	}

	public void setActionEvents(List<ActionEvent> actionEvents) {
		this.actionEvents = actionEvents;
	}

}
