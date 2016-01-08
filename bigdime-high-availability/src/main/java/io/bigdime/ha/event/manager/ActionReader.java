/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.event.manager;

import java.io.IOException;

import javax.management.modelmbean.InvalidTargetObjectTypeException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.bigdime.ha.event.manager.ActionKeys.*;

/**
 * @author jbrinnand
 */
public class ActionReader {
	private static Logger logger = LoggerFactory.getLogger(ActionReader.class);
	private JsonNode jsonNode;

	protected ActionReader (Object action) 
			throws IOException, InvalidTargetObjectTypeException { 
		if (action instanceof byte[]) {
			jsonNode = new ObjectMapper().readTree((byte[])action);
		}
	}
	public static ActionReader getInstance(Object action) 
			throws IOException, InvalidTargetObjectTypeException {
		return new ActionReader(action);
	}
	public String getType () {
		return jsonNode.get(ACTION).get(TYPE).getTextValue();
	}
	public String getSubType () {
		return jsonNode.get(ACTION).get(SUBTYPE).getTextValue();
	}	
	public String getMessage () {
		return jsonNode.get(ACTION).get(BODY).get(MESSAGE).asText();
	}	
	public String getHost () {
		if(jsonNode.get(ACTION).get(BODY).get(HOST) != null){
			return jsonNode.get(ACTION).get(BODY).get(HOST).asText();
		}
		return null;
	}
	public int getPort () {
		if(jsonNode.get(ACTION).get(BODY).get(PORT) != null){
			return jsonNode.get(ACTION).get(BODY).get(PORT).asInt();
		}
		return 0;
	}	
	public String getSource () {
		return jsonNode.get(ACTION).get(SOURCE).getTextValue();
	}	
	public String getTrail () {
		return jsonNode.get(ACTION).get(TRAIL).getTextValue();
	}	
	@Override
	public String toString() {
		return jsonNode.toString();
	}
	public Action toAction() {
		Action action = Action.getInstance()
				.type(getType())
				.subtype(getSubType())
				.source(getSource())
				.message(getMessage(), getHost(), getPort())
				.trail(getTrail())
				.build();
		return action;
	}

	public boolean validateReader() {
		boolean validJson  = false;
		if(jsonNode != null){
			if(jsonNode.get(ACTION) != null && jsonNode.get(ACTION).get(TYPE).getTextValue() != null
					&& jsonNode.get(ACTION).get(SOURCE) != null){
				validJson = true;
			}	
		}
		return validJson;
	}
}
