/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Singleton implementation of {@link AdaptorContext} interface. Provides the
 * information about the Adaptor.
 * 
 * @author Neeraj Jain
 *
 */
public final class AdaptorContextImpl implements AdaptorContext {
	private String name;
	private Collection<Source> sources;
	private Collection<Sink> sinks;
	private Collection<DataChannel> channels;

	private static AdaptorContext instance = new AdaptorContextImpl();

	public static AdaptorContext getInstance() {
		return instance;
	}

	private AdaptorContextImpl() {
	}

	@Override
	public void setAdaptorName(String name) {
		this.name = name;
	}

	@Override
	public String getAdaptorName() {
		return name;
	}

	@Override
	public Collection<Source> getSources() {
		return sources;
	}

	@Override
	public Collection<Sink> getSinks() {
		return sinks;
	}

	@Override
	public Collection<DataChannel> getChannels() {
		return channels;
	}

	@Override
	public void setSources(Collection<Source> sources) {
		this.sources = sources;
	}

	@Override
	public void setSinks(Collection<Sink> sinks) {
		this.sinks = sinks;
	}

	@Override
	public void setChannels(Collection<DataChannel> channels) {
		this.channels = channels;
	}

	@Override
	public Map<String, DataChannel> getChannelMap() {
		final Map<String, DataChannel> channelMap = new HashMap<>();
		for (DataChannel channel : getChannels()) {
			channelMap.put(channel.getName(), channel);
		}
		return channelMap;
	}
}
