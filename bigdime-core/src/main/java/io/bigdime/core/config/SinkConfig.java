/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.config;

import java.util.Collection;
import java.util.LinkedHashSet;

public class SinkConfig {
	private String name;
	private String description;
	private LinkedHashSet<HandlerConfig> handlerConfigs;
	private Collection<String> channelDescs;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public LinkedHashSet<HandlerConfig> getHandlerConfigs() {
		return handlerConfigs;
	}

	public void setHandlerConfigs(LinkedHashSet<HandlerConfig> handlers) {
		this.handlerConfigs = handlers;
	}

	public Collection<String> getChannelDescs() {
		return channelDescs;
	}

	public void setChannelDescs(Collection<String> channelDescs) {
		this.channelDescs = channelDescs;
	}

	@Override
	public String toString() {
		return "SinkConfig [name=" + name + ", description=" + description + ", handlerConfigs=" + handlerConfigs
				+ ", channelDescs=" + channelDescs + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SinkConfig other = (SinkConfig) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

}
