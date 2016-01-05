/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.config;

import java.util.Map;

/**
 *
 * @author Neeraj Jain
 *
 * @param <T>
 */
public class HandlerConfig {
	private String name;
	private String description;
	private String handlerClass;
	private Map<String, Object> handlerProperties;

	/**
	 * Gets the name of the handler.
	 *
	 * @return name of the handler
	 */
	public String getName() {
		return name;
	}

	/**
	 * Sets the name of the handler.
	 *
	 * @param name
	 *            to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Gets the high level description set for this handler.
	 *
	 * @return description of the handler
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * Sets the description of the handler
	 *
	 * @param description
	 *            to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * Gets the class that implements this {@code Handler}.
	 *
	 * @return Handler class
	 */
	public String getHandlerClass() {
		return handlerClass;
	}

	/**
	 * Sets the class that implements this {@code Handler}.
	 *
	 * @param handlerClass
	 *            to set
	 */
	public void setHandlerClass(String handlerClass) {
		this.handlerClass = handlerClass;
	}

	/**
	 * Gets the properties that are required to build the {@link handlerClass}
	 *
	 * @return
	 */
	public Map<String, Object> getHandlerProperties() {
		return handlerProperties;
	}

	/**
	 * Sets the handlerProperties
	 *
	 * @param handlerProperties
	 *            to set.
	 */
	public void setHandlerProperties(Map<String, Object> processorProperties) {
		this.handlerProperties = processorProperties;
	}

	@Override
	public String toString() {
		return "HandlerConfig [name=" + name + ", description=" + description + ", handlerClass=" + handlerClass
				+ ", handlerProperties=" + handlerProperties + "]";
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
		HandlerConfig other = (HandlerConfig) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}
}
