/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.config;

import java.util.HashMap;
import java.util.Map;

public class ChannelConfig {
	/**
	 * Name of the channel, a required field. This is referred to by source and
	 * sink elements as well.
	 */
	private String name;
	/**
	 * High level description of what this channel does.
	 */
	private String description;
	/**
	 * Processing DataChannel class.
	 */
	private String channelClass;
	/**
	 * Properties needed to initialize the {@link channelClass}.
	 */
	private Map<String, Object> channelProperties;

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

	public String getChannelClass() {
		return channelClass;
	}

	public void setChannelClass(String channelClass) {
		this.channelClass = channelClass;
	}

	public Map<String, Object> getChannelProperties() {
		if (channelProperties == null)
			channelProperties = new HashMap<>();
		return channelProperties;
	}

	public void setChannelProperties(Map<String, Object> processorProperties) {
		this.channelProperties = processorProperties;
	}

	@Override
	public String toString() {
		return "ChannelConfig [name=" + name + ", description=" + description + ", channelClass=" + channelClass
				+ ", channelProperties=" + channelProperties + "]";
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
		ChannelConfig other = (ChannelConfig) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

}
