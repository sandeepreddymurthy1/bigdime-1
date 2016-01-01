/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.line;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.handler.SimpleJournal;

public class LineHandlerJournal extends SimpleJournal {

	private ActionEvent leftoverEvent;

	public ActionEvent getLeftoverEvent() {
		return leftoverEvent;
	}

	public void setLeftoverEvent(ActionEvent leftoverEvent) {
		this.leftoverEvent = leftoverEvent;
	}

}
