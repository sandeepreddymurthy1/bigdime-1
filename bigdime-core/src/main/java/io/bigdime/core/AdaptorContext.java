/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core;

import java.util.Collection;
import java.util.Map;

/**
 * AdaptorContext defines methods that can be used to get information about the
 * running instance of data adaptor. There is one context per adaptor per JVM.
 *
 * @author Neeraj Jain
 *
 */
public interface AdaptorContext {
	/**
	 * Gets the name of the running adaptor.
	 *
	 * @return name of the adaptor
	 */
	public void setAdaptorName(String name);

	public String getAdaptorName();

	public Collection<Source> getSources();

	public Collection<Sink> getSinks();

	public Collection<DataChannel> getChannels();

	public void setSources(Collection<Source> sources);

	public void setSinks(Collection<Sink> sinks);

	public void setChannels(Collection<DataChannel> channels);

	public Map<String, DataChannel> getChannelMap();
	// public AdaptorValidationType getAdaptorValidationType();

}
