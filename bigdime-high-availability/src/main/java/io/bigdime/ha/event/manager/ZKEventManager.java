/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.event.manager;

import static io.bigdime.ha.event.manager.common.ZKEventManagerConstants.*;
import io.bigdime.ha.event.handler.INotficationHandler;
import io.bigdime.ha.event.handler.ZKEventHandlerClient;
import io.bigdime.ha.zookeeper.client.ZookeeperClient;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.modelmbean.InvalidTargetObjectTypeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * @author jbrinnand/mnamburi
 * this manager class will help to read messages from Zookeeper and send the messages to client action handlers
 */
public class ZKEventManager {
	private static ActorSystem managerSystem;
	private Cache<String, Object> cache;
	private ActorRef manager;
	private ActorRef zookeeperClient;
	private ActorRef clientEventHandler;
	private HashMap<String, ActorRef> actors;
	private AtomicInteger count;
	private boolean isActive = false;
	private Logger logger = LoggerFactory.getLogger(ZKEventManager.class); 
	private HashMap<String,INotficationHandler>  notficationHandlers = null;
	private String clientHost = null;
	private int defaultMaxSize = 1000;
	/**
	 * Construct the sub-system.
	 * 
	 * @param eventHandler
	 * @param eventHandlerName
	 * @param path
	 * @param data
	 */
	protected ZKEventManager(Class<?> actionHandler, String eventHandlerName, 
			String path,String listenerPath,HashMap<String,Class<INotficationHandler>> handlers)  {
		actors = new HashMap<String, ActorRef>();
		cache = CacheBuilder.newBuilder()
				.maximumSize(defaultMaxSize)
				.build();	
		count = new AtomicInteger();
		count.getAndIncrement();

		// Set up the ZK host and port
		String host  = System.getProperty("zk-host");
		String port  = System.getProperty("zk-port");
		if (host == null) {
			host = DEFAULT_HOST;
		}
		if (port == null) {
			port = String.valueOf(DEFAULT_PORT);
		}

		// adding the process id and ip address to the event handler name.
		//********************************
		eventHandlerName = eventHandlerName + DASH + getProcessID();

		// Set up the Client Host and port
		String clientSystemHost  = System.getProperty(CLIENT_HOST);
		String clientPort  = System.getProperty(CLIENT_PORT);
		if (clientSystemHost != null) {
			clientHost = clientSystemHost;
		}
		if (clientPort == null) {
			clientPort = String.valueOf(DEFAULT_CLIENT_PORT);
		}		
		
		String systemName = ZK_EVENT_MANAGER + "-" + eventHandlerName;
		managerSystem = ActorSystem.create(systemName);
		
		// Buid the Notfication Handler Objects
		try {
			buildNotificationHandlers(handlers);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		manager = managerSystem.actorOf(Props.create(ZKEventHandlerClient.class, 
				ZKEventManagerMonitorActionHandler.class, cache,notficationHandlers), MANAGER); 

		// Instantiate the zookeeper client.
		//*****************************
		String clientName = eventHandlerName + DASH 
				+ ZOOKEEPER_CLIENT  + DASH + count ;
		zookeeperClient = managerSystem.actorOf(Props.create(
				ZookeeperClient.class, host, 
				Integer.valueOf(port), path,clientHost,Integer.valueOf(clientPort),listenerPath), 
				clientName);
		
		// Instantiate the event handler client.
		//*****************************		
		clientEventHandler = managerSystem.actorOf(
				Props.create(ZKEventHandlerClient.class, actionHandler, cache , notficationHandlers),
				eventHandlerName);

		// Tell the actors who their peers are.
		//*****************************		
		Action action = Action.getInstance()
				.source(MANAGER)
				.type(PEER)
				.message("Peer initialization")
				.build();
		zookeeperClient.tell(action.getBytes(), clientEventHandler);
		clientEventHandler.tell(action.getBytes(), zookeeperClient);

		// Store the actors.
		//**************
		actors.put(ZOOKEEPER_CLIENT, zookeeperClient);
		actors.put(eventHandlerName,  clientEventHandler);
	}
	/**
	 * Register the event handler.
	 * 
	 * @param eventHandler
	 * @param eventHandlerName
	 * @param path
	 * @param data
	 * @return
	 */
	public static ZKEventManager getInstance(Class<?> actionHandler,
			String eventHandlerName, String path,String listenerPath,HashMap<String,Class<INotficationHandler>> notficationHandlers) {
		return new ZKEventManager(actionHandler, 
				eventHandlerName, path,listenerPath,notficationHandlers);
	}
	/**
	 * Start the actors.
	 * 
	 * @throws InterruptedException
	 */
	public void start() throws InterruptedException {
		Action action = Action.getInstance()
				.source(MANAGER)
				.type(START)
				.message("Start up requested.")
				.trail(MANAGER)
				.build();
		zookeeperClient.tell(action.getBytes(), manager);
		clientEventHandler.tell(action.getBytes(), manager);
		isActive = true;
	}
	/**
	 * Stop the actors.
	 * 
	 * @throws InterruptedException
	 */
	public void stop() throws InterruptedException {
		Action action = Action.getInstance()
				.source(MANAGER)
				.type(STOP)
				.message("Shutdown requested.")
				.trail(MANAGER)
				.build();
		clientEventHandler.tell (action.getBytes(), manager);
		isActive = false;
	}	
	public  void shutdown () {
		for (Entry<String, ActorRef> entry : actors.entrySet()) {
			ActorRef actor = entry.getValue();
			actor.tell (PoisonPill.getInstance(), ActorRef.noSender());
		}
		manager.tell (PoisonPill.getInstance(), ActorRef.noSender());
		managerSystem.shutdown();				
	}
	public void send(Object msg, String receiver, String source) throws IOException, InvalidTargetObjectTypeException {
		
		ActionReader reader = ActionReader.getInstance(msg);
		Action action = reader.toAction(); 
		action.trail(source + "/" + manager.path().name()).build();
		
		actors.get(receiver).tell(action.toString().getBytes(), manager);
	}	
	public boolean isActive () {
		return isActive;
	}
	/**
	 * ManagementFactory.getRuntimeMXBean().getName() return the ProcessID@IP_ADDRESS.
	 * We are extracting the process id from the string using @
	 * @return hostname-processid
	 */
	public String getProcessID(){
		String ip_processID = null;
		String ids[] =  ManagementFactory.getRuntimeMXBean().getName().split("@");
		Preconditions.checkNotNull(ids[0]);
		Preconditions.checkNotNull(ids[1]);
		// Akka Process Name supports  only word characters (i.e. [a-zA-Z0-9] plus non-leading '-')
		//********************************
		if(ids[1].contains(DOT)){
			ip_processID = ids[1].replace(DOT,DASH) + DASH + ids[0];
			clientHost = ids[1];
		} else {
			ip_processID = ids[1] + DASH +ids[0];
			clientHost = ids[1];
		}
		logger.info("Process Details"+ip_processID);
		return ip_processID;
	}
	public Cache<String, Object> getCache() {
		return cache;
	}
	public void setCache(Cache<String, Object> cache) {
		this.cache = cache;
	}
	public void buildNotificationHandlers(HashMap<String,Class<INotficationHandler>> handlers) throws NoSuchMethodException, IllegalArgumentException,
		InstantiationException, IllegalAccessException, 
		InvocationTargetException
	{
		if(handlers != null){
			Iterator<Entry<String, Class<INotficationHandler>>> iterator = handlers.entrySet().iterator();
			Class<INotficationHandler> class101 = null;
			INotficationHandler  notficationHandler = null;
			Constructor<?> constructor = null;
			Entry<String, Class<INotficationHandler>> notficationPair = null;
			notficationHandlers = new HashMap<String,INotficationHandler>();
			while(iterator.hasNext()){
				notficationPair = iterator.next();
				class101 = notficationPair.getValue();
				constructor = class101.getConstructor();
				notficationHandler = (INotficationHandler) constructor.newInstance();
				notficationHandlers.put(notficationPair.getKey(), notficationHandler);
			}			
		}
	}
	
	public HashMap<String,INotficationHandler> getNotificationHandlers(){
		return notficationHandlers;
	}
}
