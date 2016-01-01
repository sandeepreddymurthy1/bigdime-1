/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.channel;

import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.flume.Event;

/**
 * Kafka based channel, to be implemented.
 *
 * @author Neeraj Jain
 *
 */

public class KafkaChannel extends AbstractChannel {

	@Override
	public void build() {
		throw new NotImplementedException();
	}

	@Override
	public void put(Event arg0) {
		throw new NotImplementedException();
	}

	@Override
	public Event take() {
		throw new NotImplementedException();
	}

	@Override
	public List<Event> take(int size) {
		throw new NotImplementedException();
	}

	@Override
	public boolean registerConsumer(String consumerName) {
		throw new NotImplementedException();
	}

	@Override
	public Event take(String consumerName) {
		throw new NotImplementedException();
	}

	@Override
	public List<Event> take(String consumerName, int size) {
		throw new NotImplementedException();
	}

	@Override
	public void start() {
		throw new NotImplementedException();
	}

	@Override
	public void stop() {
		throw new NotImplementedException();
	}
}
