/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.sink;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.DataChannel;
import io.bigdime.core.Handler;
import io.bigdime.core.Sink;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.config.AdaptorConfigConstants.SinkConfigConstants;
import io.bigdime.core.config.HandlerConfig;
import io.bigdime.core.config.SinkConfig;
import io.bigdime.core.handler.HandlerFactory;

/**
 * The <code>DataSinkFactory</code> provides methods for producing Sink object
 * based on given {@link SinkConfig} object.
 * 
 * @author Neeraj Jain
 *
 */
@Component
public final class DataSinkFactory {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(DataSinkFactory.class));
	@Autowired
	private HandlerFactory handlerFactory;

	/**
	 * Creates and returns a collection DataSink objects for given
	 * {@link SinkConfig} object. There will be one Sink object added to
	 * collection for each channel-desc present in sinkConfig.
	 * 
	 * @param sinkConfig
	 * @return
	 * @throws AdaptorConfigurationException
	 */
	public Collection<Sink> getDataSink(final SinkConfig sinkConfig) throws AdaptorConfigurationException {
		if (sinkConfig == null) {
			throw new AdaptorConfigurationException("sinkConfig can't be null");
		}
		Collection<Sink> dataSinks = new HashSet<>();
		Collection<String> channelDescs = sinkConfig.getChannelDescs();
		int sinkInstance = 0;
		for (String channelDesc : channelDescs) {
			sinkInstance++;
			final DataSink dataSink = new DataSink(getHandlers(sinkConfig, channelDesc),
					sinkConfig.getName() + "-" + sinkInstance);
			dataSink.setDescription(sinkConfig.getDescription());
			dataSinks.add(dataSink);
		}
		return dataSinks;
	}

	private LinkedHashSet<Handler> getHandlers(final SinkConfig sinkConfig, final String channelDesc)
			throws AdaptorConfigurationException {
		final LinkedHashSet<Handler> handlers = new LinkedHashSet<>();
		final LinkedHashSet<HandlerConfig> handlerConfigs = sinkConfig.getHandlerConfigs();
		int handlerConfigIndex = 0;
		for (HandlerConfig handlerConfig : handlerConfigs) {
			logger.debug("parsing sink handler", "handler_config=\"{}\"", handlerConfig);
			handlerConfig.getHandlerProperties().put(SinkConfigConstants.CHANNEL_DESC, channelDesc);
			handlers.add(handlerFactory.getHandler(handlerConfig));
			Map<String, DataChannel> channelMap = AdaptorConfig.getInstance().getAdaptorContext().getChannelMap();
			boolean registerWithChannel = false;
			if (handlerConfigIndex == 0) {
				// TODO: handler NPE. If the channel is not configured with
				// channelDesc, channelMap.get(channelDesc) will throw NPE.
				registerWithChannel = channelMap.get(channelDesc).registerConsumer(handlerConfig.getName());
			}
			logger.info("built a sink",
					"channelDesc=\"{}\" handler_name=\"{}\" handlerIndex=\"{}\" registerWithChannel=\"{}\" properties=\"{}\"",
					channelDesc, handlerConfig.getName(), handlerConfigIndex, registerWithChannel,
					handlerConfig.getHandlerProperties());
			handlerConfigIndex++;
		}
		return handlers;
	}

}
