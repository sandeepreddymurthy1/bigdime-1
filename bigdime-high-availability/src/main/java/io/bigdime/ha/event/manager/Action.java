/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.event.manager;

import java.nio.charset.Charset;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import static io.bigdime.ha.event.manager.ActionKeys.*;

/**
 * @author jbrinnand
 */
public class Action {
	private ObjectNode on;
	private String actionType;
	private String message;
	private String source;
	private String trail;
	private String subType;
	private String host;
	private int port;
	
	protected Action () { 
		on = new ObjectMapper().createObjectNode();
		on.with(ACTION);
		actionType = "";
		message = "";
		source = "";
		trail = "";
		subType = "";
	}
	public static Action getInstance() {
		return new Action();
	}
	public Action type (String actionType) {
		this.actionType = actionType;
		return this;
	}
	public Action subtype (String subType) {
		this.subType = subType;
		return this;
	}	
	public Action message (String message) {
		this.message = message;
		return this;
	}
	public Action message (String message,String host,int port) {
		this.message = message;
		this.host = host;
		this.port = port;
		return this;
	}	
	public Action source (String source) {
		this.source = source;
		return this;
	}	
	public Action trail (String trail) {
		this.trail += trail;
		return this;
	}	
	public Action build () {
		on.with(ACTION).put(TYPE, actionType);
		on.with(ACTION).put(SUBTYPE, subType);
		on.with(ACTION).put(TIMESTAMP, System.currentTimeMillis());
		on.with(ACTION).put(SOURCE, source); 
		String tempTrail;
		if (trail == null || trail.equals("")) {
			tempTrail = "/" + source;
			on.with(ACTION).put(TRAIL, tempTrail); 
		}
		else {
			tempTrail =  trail + "/"; 
			on.with(ACTION).put(TRAIL,  tempTrail); 
		}
		if(host != null){
			on.with(ACTION).with(BODY).put(HOST, host);
			on.with(ACTION).with(BODY).put(PORT, port);
		}
		on.with(ACTION).with(BODY).put(MESSAGE, message);
		trail = tempTrail; 
		return this;
	}
	public ObjectNode get() {
		return on;
	}
	@Override
	public String toString() {
		return on.toString();
	}
	public byte[] getBytes() {
		return on.toString().getBytes(Charset.defaultCharset());
	}
}
