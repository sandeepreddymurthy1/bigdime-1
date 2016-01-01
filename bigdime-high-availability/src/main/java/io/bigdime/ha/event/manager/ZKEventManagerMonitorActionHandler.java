/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.event.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;

import akka.actor.ActorRef;


/**
 * @author jbrinnand
 */
public class ZKEventManagerMonitorActionHandler extends ActionHandler {
	private static Logger logger = LoggerFactory.getLogger(ZKEventManagerMonitorActionHandler.class);
	private ActorRef self;
	
	// This class is a place holder for a watcher for all the events 
	// taking place in the big data zookeeper cluster.
	//************************************************	
	public ZKEventManagerMonitorActionHandler (Cache<String,Object> cache) {}
	
	public void setSelf (ActorRef self) {
		
		this.self = self;
		logger. info ("Setting the reference to self" + this.self.path().name());
	}
}
