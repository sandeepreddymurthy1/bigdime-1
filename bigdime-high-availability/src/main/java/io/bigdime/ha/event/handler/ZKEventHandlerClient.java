/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.event.handler;

import io.bigdime.ha.event.client.exception.ZKEventHandlerClientException;
import io.bigdime.ha.event.manager.IActionHandler;

import java.lang.reflect.Constructor;
import java.util.HashMap;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import com.google.common.cache.Cache;

/**
 * @author jbrinnand
 */
public class ZKEventHandlerClient extends UntypedActor {
	private LoggingAdapter logger = Logging.getLogger(getContext().system(), this);
	private IActionHandler actionHandler;
	private ZKEventHandler eventHandler;
	
	public ZKEventHandlerClient(Class<?> actionHandlerClazz,
			Cache<String, Object> cache,HashMap<String,INotficationHandler> notificationHandlers) throws ZKEventHandlerClientException {
		super();
		try {
			Constructor<?> constructor = actionHandlerClazz.getConstructor(Cache.class);
			actionHandler = (IActionHandler) constructor.newInstance(cache);
			actionHandler.setNotficationHandlers(notificationHandlers);
			logger.info("ZKEventHandlerName"+getSelf().path().name());
			actionHandler.setSelf(getSelf());
		} catch (Exception e) {
			throw new ZKEventHandlerClientException(e.getMessage(), e);
		}
		logger.info("Instantiating actionHandler {}", actionHandler.toString()
				.getClass().getName());
		eventHandler = new ZKEventHandler(actionHandler);
	}

	@Override
	public void onReceive(Object msg) throws Exception {
		logger.debug("Forward the message to eventHandler");
		eventHandler.onReceive(msg, getSelf(), sender());
	}
}
