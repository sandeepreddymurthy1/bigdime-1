/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.util;

import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A queue that support multiple consumers. Item at index i can be polled by
 * multiple consumers and when all the consumers are done polling the item, it's
 * removed from the queue.
 * 
 * @author Neeraj Jain
 * @param <E>
 *
 */
public class MultipleConsumerQueue<E> extends AbstractQueue<E> implements SubscriberQueue<E> {
	private Set<String> consumerNames = new HashSet<>();
	/*
	 * DataChannel maintains the data in an array list, need random access to
	 * the elements.
	 */
	private List<E> itemList = Collections.synchronizedList(new ArrayList<E>());
	/**
	 * A map that maintains the map between consumerName and the index that was
	 * polled last time.
	 */
	private Map<String, Integer> consumerPolledIndexMap = new HashMap<>();
	/*
	 * Maintains a mapping between the index in the itemList and how many times
	 * it has been consumed. Once the element has been consumed by all the
	 * consumers, the poll method should remove it from the list.
	 */
	private Map<Integer, Integer> indexToPolledCountMap = new HashMap<>();

	/*
	 * Number of elements that have been removed from the itemList. Need this to
	 * compute the next index.
	 */
	private int removedCount;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * io.bigdime.core.channel.SubscriberQueue#addConsumer(java.lang.String)
	 */
	@Override
	public boolean registerConsumer(String consumerName) {
		return consumerNames.add(consumerName);
	}

	/**
	 * Removes the consumer from the list and adjusts the counts.
	 * 
	 * @param consumerName
	 * @return
	 */
	@Override
	public boolean unregisterConsumer(String consumerName) {
		validateConsumer(consumerName);
		/*
		 * for any items that this consumer has polled, decrement the
		 * polledCount.
		 */
		int absolutePollIndex = getAbsolutePollIndex(consumerName);
		for (int i = removedCount; i < absolutePollIndex; i++) {
			Integer count = indexToPolledCountMap.get(i);
			if (count != null) {
				count--;
				if (count == 0) {
					indexToPolledCountMap.remove(i);
				} else
					indexToPolledCountMap.put(i, count);
			}
		}
		consumerPolledIndexMap.remove(consumerName);
		boolean removed = consumerNames.remove(consumerName);
		for (Iterator<E> iterator = itemList.iterator(); iterator.hasNext();) {
			iterator.next();
			if (indexToPolledCountMap.get(removedCount) != null
					&& indexToPolledCountMap.get(removedCount) == consumerNames.size()) {
				iterator.remove();
				indexToPolledCountMap.remove(removedCount);
				removedCount++;
			}
		}
		return removed;
	}

	@Override
	public boolean offer(E e) {
		return itemList.add(e);
	}

	@Override
	public E poll() {
		if ((consumerNames == null) || consumerNames.isEmpty()) {
			return poll("default");
		} else {
			throw new UnsupportedOperationException(
					"this queue has registered consumers, invoking poll method without parameters is not supported.");
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.bigdime.core.channel.SubscriberQueue#poll(java.lang.String)
	 */
	@Override
	public E poll(String consumerName) {
		List<E> items = poll(consumerName, 1);
		return items.get(0);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.bigdime.core.channel.SubscriberQueue#poll(int)
	 */
	@Override
	public List<E> poll(int size) {
		if ((consumerNames == null) || consumerNames.isEmpty()) {
			return poll("default", size);
		} else {
			throw new UnsupportedOperationException(
					"this queue has registered consumers, invoking poll method without parameters is not supported.");
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.bigdime.core.channel.SubscriberQueue#poll(java.lang.String, int)
	 */
	@Override
	public synchronized List<E> poll(String consumerName, int size) {
		validateConsumer(consumerName);
		Integer absolutePollIndex = getAbsolutePollIndex(consumerName);
		int newPollStartIndex = getPollStartIndex(consumerName);
		int availableItemCount = itemList.size() - newPollStartIndex;
		if (newPollStartIndex == -1) {
			return null;
		}
		int fetchSize = size;
		if (availableItemCount < size) {
			fetchSize = availableItemCount;
		}
		int newPollEndIndex = newPollStartIndex + fetchSize;
		List<E> polledItem = new ArrayList<>(itemList.subList(newPollStartIndex, newPollEndIndex));
		consumerPolledIndexMap.put(consumerName, (absolutePollIndex + fetchSize) - 1);

		// start:update the taken count for this index
		for (int i = 0; i < fetchSize; i++) {
			Integer polledCount = indexToPolledCountMap.get(absolutePollIndex + i);
			if (polledCount == null) {
				polledCount = 0;
			}
			polledCount++;
			if (polledCount == consumerNames.size()) { // newTakenIndex must
														// always be zero
				itemList.remove(0);
				indexToPolledCountMap.remove(absolutePollIndex + i);
				removedCount++;
			} else {
				indexToPolledCountMap.put(absolutePollIndex + i, polledCount);
			}

		}
		return polledItem;
	}

	/**
	 * Get the absolute index that should be polled now. If the last polled
	 * index was n, then this method return n+1. If this is the first call from
	 * this consumer, then this method will return 0.
	 * 
	 * @param consumerName
	 * @return
	 */
	private int getAbsolutePollIndex(String consumerName) {
		Integer polledIndex = consumerPolledIndexMap.get(consumerName);
		if (polledIndex == null) {
			polledIndex = 0;
		} else {
			polledIndex++;
		}
		return polledIndex;
	}

	private int getRelativePollIndex(String consumerName) {
		Integer absolutePollIndex = getAbsolutePollIndex(consumerName);
		int newPollStartIndex = absolutePollIndex - removedCount;
		return newPollStartIndex;
	}

	/**
	 * Computes the index from where item will be polled.
	 * 
	 * @param consumerName
	 * @return index from where item will be polled. Returns -1 if there is no
	 *         item available to poll for this consumer.
	 */
	private int getPollStartIndex(String consumerName) {
		int newPollStartIndex = getRelativePollIndex(consumerName);
		int availableItemCount = itemList.size() - newPollStartIndex;
		if (availableItemCount == 0) {
			return -1;
		}
		return newPollStartIndex;
	}

	/**
	 * Get the next item that will be polled by the default consumer.
	 */
	public E peek() {
		if ((consumerNames == null) || consumerNames.isEmpty()) {
			return peek("default");
		} else {
			throw new UnsupportedOperationException(
					"this queue has registered consumers, invoking peek method without parameters is not supported.");
		}
	}

	/*
	 * 
	 * 
	 * @see io.bigdime.core.channel.SubscriberQueue#peek(java.lang.String)
	 */

	/**
	 * Get the next item without removing from the queue that will be polled by
	 * the consumer specified by the consumerName.
	 */
	@Override
	public E peek(String consumerName) {
		validateConsumer(consumerName);
		/*
		 * @formatter:off
		 * Say, list size =5.
		 * consumerName has polled 2 items before.
		 * polledIndex = 2
		 * @formatter:on
		 */
		final Integer pollIndex = getAbsolutePollIndex(consumerName);
		int newPollStartIndex = pollIndex - removedCount;
		int availableItemCount = itemList.size() - newPollStartIndex;
		if (availableItemCount == 0) {
			return null;
		}
		return itemList.get(newPollStartIndex);
	}

	@Override
	public Iterator<E> iterator() {
		// return itemList.iterator();
		return new Itr();
	}

	@Override
	public int size() {
		if ((consumerNames == null) || consumerNames.isEmpty()) {
			return size("default");
		} else {
			throw new UnsupportedOperationException(
					"this queue has registered consumers, invoking size method without parameters is not supported.");
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.bigdime.core.channel.SubscriberQueue#size(java.lang.String)
	 */
	@Override
	public int size(String consumerName) {
		validateConsumer(consumerName);
		return itemList.size();
	}

	public Set<String> getConsumerNames() {
		return Collections.unmodifiableSet(consumerNames);
	}

	private void validateConsumer(String consumerName) {
		if (consumerName.equals("default") || consumerNames.contains(consumerName))
			return;
		throw new IllegalArgumentException("consumerName:" + consumerName + " is not registered");
	}

	private class Itr implements Iterator<E> {

		Iterator<E> it = itemList.iterator();
		// int nextItemIndex;

		@Override
		public boolean hasNext() {
			return it.hasNext();
			// if (nextItemIndex >= itemList.size())
			// return false;
			// return true;
		}

		@Override
		public E next() {
			return it.next();

			// if (nextItemIndex >= itemList.size())
			// throw new NoSuchElementException();
			// E item = itemList.get(nextItemIndex);
			// nextItemIndex++;
			// return item;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
