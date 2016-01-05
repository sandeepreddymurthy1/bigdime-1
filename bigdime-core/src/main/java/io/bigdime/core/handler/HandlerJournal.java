/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import java.util.List;

import io.bigdime.core.ActionEvent;

public interface HandlerJournal {

	long getTotalRead();

	void setTotalRead(long totalRead);

	long getTotalSize();

	void setTotalSize(long totalSize);

	List<ActionEvent> getEventList();

	void setEventList(List<ActionEvent> eventList);

}