/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.event.handler;

import java.util.List;

/**
 * 
 * @author mnamburi
 *
 */
public interface INotficationHandler {
	public void send(String json, String serviceName);
	public void register(Object nodeName);
	public void unRegister(Object nodeName);
	public int totalActiveNodes();
	public List<?> getList();
}
