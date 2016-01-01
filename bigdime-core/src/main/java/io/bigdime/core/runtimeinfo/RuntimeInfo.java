/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.runtimeinfo;

import java.util.Map;

import io.bigdime.core.runtimeinfo.RuntimeInfoStore.Status;

/**
 * Encapsulates information that needs to be serialized by adaptor by the
 * RuntimeInfo store.
 * 
 * @author Neeraj Jain
 *
 */
public class RuntimeInfo {

	private String runtimeId;
	private String adaptorName;
	private String entityName;
	private String inputDescriptor;
	private Status status;
	private String numOfAttempts;
	private Map<String, String> properties;

	public String getRuntimeId() {
		return runtimeId;
	}

	public void setRuntimeId(String runtimeId) {
		this.runtimeId = runtimeId;
	}

	public String getAdaptorName() {
		return adaptorName;
	}

	public void setAdaptorName(String adaptorName) {
		this.adaptorName = adaptorName;
	}

	public String getEntityName() {
		return entityName;
	}

	public void setEntityName(String entityName) {
		this.entityName = entityName;
	}

	public String getInputDescriptor() {
		return inputDescriptor;
	}

	public void setInputDescriptor(String inputDescriptor) {
		this.inputDescriptor = inputDescriptor;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public String getNumOfAttempts() {
		return numOfAttempts;
	}

	public void setNumOfAttempts(String numOfAttempts) {
		this.numOfAttempts = numOfAttempts;
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}

	@Override
	public String toString() {
		return "RuntimeInfo [runtimeId=" + runtimeId + ", adaptorName=" + adaptorName + ", entityName=" + entityName
				+ ", inputDescriptor=" + inputDescriptor + ", status=" + status + ", numOfAttempts=" + numOfAttempts
				+ ", properties=" + properties + "]";
	}

}
