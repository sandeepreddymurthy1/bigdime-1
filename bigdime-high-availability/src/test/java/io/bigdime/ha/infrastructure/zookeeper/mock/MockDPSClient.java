/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.infrastructure.zookeeper.mock;

import io.bigdime.ha.event.handler.INotficationHandler;
import io.bigdime.ha.event.manager.ActionReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.modelmbean.InvalidTargetObjectTypeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author mnamburi
 *
 */
public class MockDPSClient implements INotficationHandler {
	Logger logger = LoggerFactory.getLogger(MockDPSClient.class);

	List<MockServiceNode> list = new ArrayList<MockServiceNode>();
	private final AtomicInteger   index = new AtomicInteger(0);
	private MockServiceNode mockServiceNode  = null;
	@Override
	public void send(String json, String serviceName) {
		int  thisIndex = Math.abs(index.getAndIncrement());
		list.get(thisIndex % list.size());
	}

	@Override
	public int totalActiveNodes() {
		return list.size();
	}
	@Override
	public void register(Object nodeName) {

		ActionReader actionReader;
		try {
			actionReader = ActionReader.getInstance(nodeName);
			mockServiceNode = MockServiceNode.getInstance(actionReader.getHost(), actionReader.getPort(), actionReader.getSource());
			list.add(mockServiceNode);				
		} catch (IOException e) {
			logger.error(
					"_message=\"Json Parser Error\" error={} ",e);
		} catch (InvalidTargetObjectTypeException e) {
			logger.error(e.getLocalizedMessage());
		}
	}

	@Override
	public void unRegister(Object nodeName) {
		try {
			ActionReader actionReader = ActionReader.getInstance(nodeName);
			mockServiceNode = MockServiceNode.getInstance(actionReader.getHost(), actionReader.getPort(), actionReader.getSource());
			list.remove(mockServiceNode);
		} catch (Exception e) {
			logger.error(
					"_message=\"Json Parser Error\" error={} ",e);
		}
	}
	@Override
	public List<MockServiceNode> getList(){
		return list;
	}
}
