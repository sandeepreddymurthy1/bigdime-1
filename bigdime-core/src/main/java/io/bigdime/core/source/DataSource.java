/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.source;

import java.util.LinkedHashSet;
import java.util.Observable;

import org.apache.flume.lifecycle.LifecycleState;

import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.Handler;
import io.bigdime.core.Source;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.handler.AbstractHandlerManagerContainer;
import io.bigdime.core.handler.HandlerManager;

/**
 * This implementation of Source interface works with source handlers. Source is
 * built by reading the configuration and starts the HandlerManager upon start.
 *
 * @author Neeraj Jain
 *
 */
public class DataSource extends AbstractHandlerManagerContainer implements Source {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(DataSource.class));

	private String description;

	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * Creates a new DataSource given the handlers and the name.
	 *
	 * @param handlers
	 *            set of handlers that will read and process the data
	 * @param name
	 *            name of the data source
	 * @throws AdaptorConfigurationException
	 *             if handler is null or empty
	 */
	public DataSource(final LinkedHashSet<Handler> handlers, String name) throws AdaptorConfigurationException {
		logger.info("building source", "name=\"{}\"", name);
		if ((handlers == null) || handlers.isEmpty()) {
			logger.alert(ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.INVALID_ADAPTOR_CONFIGURATION,
					ALERT_SEVERITY.BLOCKER, "handlers not configured");
			throw new AdaptorConfigurationException("null or empty collection not allowed for handlers");
		}
		setHandlers(handlers);
		setName(name);

		setHandlerManager(new HandlerManager(handlers, name));
	}

	@Override
	public String toString() {
		return "DataSource [handlers=" + getHandlers() + ", name=" + getName() + ", description=" + description + "]";
	}

	/**
	 * Update method from {@link Observer} interface.
	 * 
	 * @param observable
	 *            the {@link Observable} object, which is DataSink in this case
	 * @param object
	 *            Object containing the event information, which is an Exception
	 *            object in this case
	 */
	@Override
	public void update(Observable observable, Object object) {
		logger.warn("failure event received", "object=\"{}\" arg=\"{}\"", observable, object);
		setLifecycleState(LifecycleState.ERROR);
		stop();
	}

	@Override
	protected String getContainerType() {
		return "source";
	}

}
