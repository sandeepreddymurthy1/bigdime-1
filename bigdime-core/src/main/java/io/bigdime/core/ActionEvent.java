/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core;

import java.nio.charset.Charset;

import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;

/**
 * ActionEvent extends {@link SimpleEvent} and used in several method
 * signatures.
 *
 * @author Neeraj Jain
 */
public class ActionEvent extends SimpleEvent {
	private Status status = Status.READY;

	public ActionEvent() {

	}

	public ActionEvent(Status status) {
		this.status = status;
	}

	public ActionEvent(Event event) {
		setBody(event.getBody());
		setHeaders(event.getHeaders());
	}

	@Override
	public String toString() {
		return "[Event headers = " + getHeaders() + ", body = " + new String(getBody(), Charset.defaultCharset())
				+ "status=" + status + " ]";
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public static enum Status {
		/**
		 * Done processing the current data set(file, or message etc).
		 */
		READY,

		/**
		 * Partially done processing the current data set, callback before
		 * sending new data.
		 */
		CALLBACK,

		/**
		 * No data available, cool off.
		 */
		BACKOFF
	}

	public Status getStatus() {
		return status;
	}

	public static ActionEvent newBackoffEvent() {
		return new ActionEvent(Status.BACKOFF);
	}

}
