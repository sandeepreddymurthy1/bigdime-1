/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.adaptor;

import io.bigdime.core.AdaptorContext;
import io.bigdime.core.DataAdaptorException;
import io.bigdime.core.config.AdaptorConfig;

/**
 * Defines methods for an Adaptor that runs on bigdime.
 *
 * @author Neeraj Jain
 *
 */
public interface Adaptor {
	/**
	 * Starts the data ingestion process.
	 * 
	 * @return true if the adaptor was started, false otherwise
	 * @throws DataAdaptorException
	 *             if there was an error encountered during start
	 */
	public boolean start() throws DataAdaptorException;

	/**
	 * Sends the stop signal to all the downstream components.
	 *
	 * @throws DataAdaptorException
	 *             if there was an error encountered during stop
	 */
	public void stop() throws DataAdaptorException;

	/**
	 * Sends the suspend signal to all the downstream components.
	 *
	 * @throws DataAdaptorException
	 */
	// public void suspend() throws DataAdaptorException;

	/**
	 * Sends the resume signal to all the downstream components.
	 *
	 * @throws DataAdaptorException
	 */
	// public void resume() throws DataAdaptorException;

	/**
	 * Gets the context of the running adaptor.
	 *
	 * @return an instance of AdaptorContext encapsulating adaptor's runtime
	 *         information
	 */
	public AdaptorContext getAdaptorContext();

	/**
	 * Gets the config that was provided to startup the adaptor
	 *
	 * @return AdaptorConfig instance representing the configuration parameters
	 */
	public AdaptorConfig getAdaptorConfig();

}
