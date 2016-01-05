/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.channel;

import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.flume.Transaction;
import org.apache.flume.lifecycle.LifecycleState;

import io.bigdime.core.DataChannel;

public abstract class AbstractChannel implements DataChannel {
	private Map<String, Object> propertyMap;
	private String name;

	@Override
	public Transaction getTransaction() {
		throw new NotImplementedException();
	}

	@Override
	public LifecycleState getLifecycleState() {
		throw new NotImplementedException();
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void setName(String name) {
		this.name = name;

	}

	@Override
	public void setPropertyMap(Map<String, Object> propertyMap) {
		this.propertyMap = propertyMap;
	}

	@Override
	public Map<String, Object> getProperties() {
		return propertyMap;
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
		AbstractChannel other = (AbstractChannel) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}
}
