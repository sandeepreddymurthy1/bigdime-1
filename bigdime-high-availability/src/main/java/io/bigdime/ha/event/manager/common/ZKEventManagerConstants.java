/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.event.manager.common;

public final class ZKEventManagerConstants {
	private ZKEventManagerConstants(){}
	public static final String ZOOKEEPER_CLIENT = "zookeeper-client";
	public static final String ZK_EVENT_MANAGER = "zk-event-manager"; 
	public static final String ZOOKEEPER = "zookeeper"; 
	public static final String ZK_CLIENT_LISTENER = "zookeeperClientListener";
	public static final String ZK_CLIENT_TREECACHE_LISTENER = "zookeeperClientTreeCacheListener";
	
	public static final String MANAGER = "manager"; 
	public static final String DEFAULT_HOST = "127.0.0.1";
	public static final int DEFAULT_PORT = 2181;
	
	//*********************************
	// Actions
	//*********************************
	public static final String START = "Start"; 
	public static final String STOP = "Stop"; 
	public static final String PEER = "Peer"; 
	public static final String REGISTRATION = "Registration"; 
	public static final String PUBLISH = "Publish"; 
	public static final String NOTIFICATION = "Notification"; 
	public static final String LEADER_ELECTION = "LeaderElection"; 
	public static final String IS_LEADER = "IsLeader"; 
	public static final String WRITE = "Write"; 

	//*********************************
	// Environment
	//*********************************	
	public static final String ZK_ENABLED = "zkEnabled"; 
	public static final String ZK_HOST = "zk-host";
	public static final String ZK_PORT = "zk-port";
	public static final String ZKS_CLIENT_NAME = "zks:client-name";
	public static final String ZKS_CLIENT_PATH = "zks:client-path";
	public static final String ZKS_CLIENT_CLASS = "zks:client-class";
	public static final String ZKS_NOTIFICATION_CLIENT_CLASS = "zks:notification-client-class";
	public static final String ZKS_NOTIFICATION_CLIENT_NAME = "zks:notification-client-name";
	public static final String NAME = "name";
	public static final String CLASS = "class";
	public static final String ZKS_CLIENT_LISTENER_PATH = "zks:client-listener-path";
	public static final String ZKS_CLIENT_SERVICE_DISCOVERY = "zks:client-service-discovery";
	public static final String CLIENT_HOST = "client-host";
	public static final String CLIENT_PORT = "client-port";
	
	public static final int DEFAULT_CLIENT_PORT = 8080;
	
	//*********************************
	// Constants
	//*********************************	
	public static final String DASH = "-";
	public static final String DOT = ".";
	//*********************************
	// Servlet Context Key
	//*********************************	
	public static final String ZKEVENT_MANAGER_KEY = "io.bigdime.ha.zookeeper.event.manager.KEY";
}
