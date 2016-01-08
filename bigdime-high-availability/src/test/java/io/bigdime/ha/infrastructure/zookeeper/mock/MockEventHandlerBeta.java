/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.infrastructure.zookeeper.mock;

import io.bigdime.ha.event.handler.ZKEventHandler;
import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import com.google.common.cache.Cache;

/**
 * @author jbrinnand
 */
public class MockEventHandlerBeta extends UntypedActor {
	private LoggingAdapter logger = Logging.getLogger(getContext().system(), this);
	private MockEventHandlerBetaActionHandler actionHandler;
	private ZKEventHandler eventHandler;
	
	public MockEventHandlerBeta (Cache<String, ActorRef> cache) {
		actionHandler = new MockEventHandlerBetaActionHandler(getSelf());
		eventHandler = new ZKEventHandler(actionHandler);
	}

	@Override
	public void onReceive(Object msg) throws Exception {
		logger.debug("Forward the message to eventHandler");
		eventHandler.onReceive(msg, getSelf(), sender());
	}
}
