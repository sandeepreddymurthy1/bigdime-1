/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.util;

import java.util.List;

public interface SubscriberQueue<E> {

	/**
	 * Register the consumer for this queue.
	 * 
	 * @param consumerName
	 *            name of the consumer
	 * @return true if the registration was successful, false otherwise
	 */
	boolean registerConsumer(String consumerName);

	/**
	 * Unregister the consumer for this queue.
	 * 
	 * @param consumerName
	 *            name of the consumer
	 * @return true if the queue contained this consumer and was able to
	 *         unregister it, false otherwise
	 */
	boolean unregisterConsumer(String consumerName);

	/**
	 * Polls the next item from queue for this consumer.
	 * 
	 * @param consumerName
	 *            name of the consumer
	 * @return polled item
	 */
	E poll(String consumerName);

	/**
	 * Polls number of items defined by size from the queue. If the queue
	 * contained less than asked items, that many items will be returned.
	 * 
	 * @param size
	 *            number of items to be polled
	 * @return List of items limited to size
	 * @throws UnsupportedOperationException
	 *             if there are consumers registered to this queue
	 */
	List<E> poll(int size) throws UnsupportedOperationException;

	/**
	 * Polls number of items for this consumer defined by size from the queue.
	 * If the queue contained less than asked items, that many items will be
	 * returned.
	 * 
	 * @param consumerName
	 *            name of the consumer
	 * @param size
	 *            number of items to be polled
	 * @return List of items limited to size
	 */
	List<E> poll(String consumerName, int size);

	/**
	 * Get the next item without removing from the queue that will be polled by
	 * the consumer specified by the consumerName.
	 * 
	 * @param consumerName
	 * @return
	 */
	E peek(String consumerName);

	/**
	 * Get the size of the queue for the consumer specified by consumerName.
	 * 
	 * @param consumerName
	 *            name of the consumer for which the size of the queue needs to
	 *            be computed
	 * @return size of the queue for given consumer
	 */
	int size(String consumerName);

}