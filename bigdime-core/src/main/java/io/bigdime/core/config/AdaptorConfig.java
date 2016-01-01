/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.config;

import java.util.Set;

import io.bigdime.core.AdaptorContext;
import io.bigdime.core.AdaptorContextImpl;

/**
 * Object encapsulating adaptor's configuration.
 * 
 * @author Neeraj Jain
 *
 */
public final class AdaptorConfig {

	private static final AdaptorConfig instance = new AdaptorConfig();

	private AdaptorConfig() {

	}

	public static AdaptorConfig getInstance() {
		return instance;
	}

	/**
	 * Namespace of the organization that owns this adaptor.
	 */
	private String namespace;
	/**
	 * Name of the data adaptor, should be unique within the given namespace.
	 */
	// private String name;

	/**
	 * Type of the adaptor.
	 */
	private ADAPTOR_TYPE type;
	/**
	 * High level description of what this adaptor does.
	 */
	private String description;
	/**
	 * Instance of Source that reads data from this adaptor's data sourceConfig.
	 */
	private SourceConfig sourceConfig;
	/**
	 * One or more channelConfigs where the data can be temporarily stored for
	 * further processing. Data from one input can only be sent to one
	 * channelConfigs.
	 */
	private Set<ChannelConfig> channelConfigs;
	/**
	 * One or more sinkConfigs to process the data. More than one sinkConfigs
	 * can read the data from the same channelConfigs, in case different
	 * sinkConfigs want to store the data on different locations. One
	 * sinkConfigs can read data from multiple channelConfigs too, say all the
	 * raw data from all the channelConfigs need to be stored in hdfs.
	 */
	private Set<SinkConfig> sinkConfigs;

	/**
	 * If the adaptor is of type "batch", the start method is invoked based on
	 * cronExpression.
	 */
	private String cronExpression;

	private boolean autoStart;

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public String getName() {
		if (getAdaptorContext().getAdaptorName() == null)
			return "NO-NAME";
		return getAdaptorContext().getAdaptorName();
	}

	public void setName(String name) {
		// this.name = name;
		getAdaptorContext().setAdaptorName(name);
	}

	public ADAPTOR_TYPE getType() {
		return type;
	}

	public void setType(ADAPTOR_TYPE type) {
		this.type = type;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public SourceConfig getSourceConfig() {
		return sourceConfig;
	}

	public void setSourceConfig(SourceConfig sourceConfig) {
		this.sourceConfig = sourceConfig;
	}

	public Set<ChannelConfig> getChannelConfigs() {
		return channelConfigs;
	}

	public void setChannelConfigs(Set<ChannelConfig> channelConfigs) {
		this.channelConfigs = channelConfigs;
	}

	public Set<SinkConfig> getSinkConfigs() {
		return sinkConfigs;
	}

	public void setSinkConfigs(Set<SinkConfig> sinkConfigs) {
		this.sinkConfigs = sinkConfigs;
	}

	@Override
	public String toString() {
		return "AdaptorConfig [namespace=" + namespace + ", name=" + getAdaptorContext().getAdaptorName()
				+ ", description=" + description + ", cronExpression=" + cronExpression + ", autoStart=" + autoStart
				+ ", sourceConfig=" + sourceConfig + ", channelConfigs=" + channelConfigs + ", sinkConfigs="
				+ sinkConfigs + "]";
	}

	private AdaptorContext adaptorContext = null;

	/**
	 * Get the application level context.
	 */
	public AdaptorContext getAdaptorContext() {
		return adaptorContext0();
	}

	private AdaptorContext adaptorContext0() {
		if (adaptorContext == null) {
			adaptorContext = AdaptorContextImpl.getInstance();
		}
		return adaptorContext;
	}

	public String getCronExpression() {
		return cronExpression;
	}

	public void setCronExpression(String cronExpression) {
		this.cronExpression = cronExpression;
	}

	public boolean isAutoStart() {
		return autoStart;
	}

	public void setAutoStart(boolean autoStart) {
		this.autoStart = autoStart;
	}

}
