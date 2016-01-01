/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.infrastructure.zookeeper.mock;

import io.bigdime.ha.event.handler.INotficationHandler;
import io.bigdime.ha.event.manager.ActionReader;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author mnamburi
 *
 */
public class MockDVSClient implements INotficationHandler {
	Logger logger = LoggerFactory.getLogger(MockDVSClient.class);
	List<MockServiceNode> list = new ArrayList<MockServiceNode>();
    private MockServiceNode mockServiceNode = null;
	@Override
	public int totalActiveNodes() {
		return list.size();
	}
	@Override
	public void register(Object nodeName) {
		try {
			ActionReader actionReader = ActionReader.getInstance(nodeName);
			mockServiceNode = MockServiceNode.getInstance(actionReader.getHost(), actionReader.getPort(), actionReader.getSource());
			list.add(mockServiceNode);
		} catch (Exception e) {
			logger.error(
					"_message=\"Json Parser Error\" error={} ",e);
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
	@Override
	public void send(String json, String serviceName) {
		// Round Robin logic here.
		
	}
}
