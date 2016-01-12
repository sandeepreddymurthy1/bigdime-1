/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core;

import java.util.List;
import java.util.Map;

import org.apache.flume.Event;

/**
 * Extends {@link org.apache.flume.Channel} interface, and declares more methods
 * to be used for BigDime implementation.
 * 
 * @author Neeraj Jain
 * 
 */
public interface DataChannel extends org.apache.flume.Channel {

	/**
	 * Builds the implementation class from configuration.
	 */
	public void build();

	/**
	 * Properties needed to build an instance of DataChannel.
	 * 
	 * @param propertyMap
	 */
	public void setPropertyMap(Map<String, Object> propertyMap);

	/**
	 * Register a new consumer for this channel.
	 * 
	 * @param consumerName
	 *            unique name of the consumer.
	 * @return true if the registration was successful, false otherwise.
	 */
	public boolean registerConsumer(String consumerName);

	/**
	 * Returns this DataChannel's properties.
	 * 
	 * @return
	 */
	public Map<String, Object> getProperties();

	/**
	 * Returns an Event from the channel.This method does not take a consumer
	 * name so if there are more than one consumers registered with the channel,
	 * this method will throw an exception.
	 * 
	 * @throws UnsupportedOperationException
	 *             if there are consumers registered to this channel.
	 */
	@Override
	public Event take();

	/**
	 * Returns a list of Event object up to size specified by the argument. This
	 * method does not take a consumer name so if there are more than one
	 * consumers registered with the channel, this method will throw an
	 * exception.
	 * 
	 * @param size
	 *            requested number of events
	 * @return List of Event objects
	 * @throws ChannelException
	 *             if there was any problem during the retrieval, such as no
	 *             items found on the channel
	 */
	public List<Event> take(int size);

	/**
	 * Returns an Event from the channel.
	 * 
	 * @param consumerName
	 *            tells the channel implementation as to who is requesting for
	 *            data.
	 * @return next Event object for the given consumer
	 * @throws ChannelException
	 *             If there is no data available in the storage for the given
	 *             consumer
	 */
	public Event take(String consumerName);

	/**
	 * Returns a list of Event objects from the channel for the given
	 * consumerName.
	 * 
	 * @param consumerName
	 *            tells the channel implementation as to who is requesting for
	 *            data.
	 * @param size
	 *            requested number of items in the list
	 * @return List of Event objects if the storage has data
	 * @throws ChannelException
	 *             if there is no data avaialble on the storage
	 */
	public List<Event> take(final String consumerName, final int size);
}
