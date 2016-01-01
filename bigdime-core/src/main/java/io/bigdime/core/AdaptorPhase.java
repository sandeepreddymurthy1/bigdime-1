/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core;

/**
 * using this enum to maintain the adaptor's phase.
 *
 * @author Neeraj Jain
 *
 */
public enum AdaptorPhase {
	/*
	 * data adaptor initializing.
	 */
	INIT("initializing adaptor"),

	/**
	 * Config reader is reading and parsing the configuration file.
	 */
	PARSING_CONFIG("parsing adaptor configuration"),

	/**
	 * Configuration file has been read, now building the adaptor's components.
	 */
	BUILDING_ADAPTOR("building adaptor components"),

	/*
	 * Adaptor has been built, now starting data adaptor.
	 */
	STARTING("starting adaptor"),
	/*
	 * Adaptor has been started.
	 */
	STARTED("adaptor has been started"),
	/*
	 * Adaptor is being stopped.
	 */
	STOPPING("stopping adaptor"),
	/*
	 * Adaptor is being stopped.
	 */
	STOPPED("adaptor has been stopped");

	private String value;

	private AdaptorPhase(String value) {
		this.value = value;
	}

	/**
	 * Gets the printable string.
	 *
	 * @return
	 */
	public String getValue() {
		return value;
	}
}