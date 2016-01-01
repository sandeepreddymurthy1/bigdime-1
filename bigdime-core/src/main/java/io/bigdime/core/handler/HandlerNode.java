/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import io.bigdime.core.Handler;

public class HandlerNode {

	private Handler handler;
	private HandlerNode next;

	public HandlerNode(Handler handler) {
		this.handler = handler;
	}

	public Handler getHandler() {
		return handler;
	}

	public HandlerNode getNext() {
		return next;
	}

	public void setNext(HandlerNode next) {
		this.next = next;
	}

}
