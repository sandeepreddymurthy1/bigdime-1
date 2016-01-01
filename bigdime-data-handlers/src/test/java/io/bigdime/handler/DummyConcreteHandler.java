/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler;

import java.nio.charset.Charset;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.config.AdaptorConfigConstants.SourceConfigConstants;
import io.bigdime.core.handler.AbstractHandler;

/**
 * @author Neeraj Jain
 *
 */

@Component
@Scope("prototype")
public class DummyConcreteHandler extends AbstractHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(DummyConcreteHandler.class));
	private String inputSrcDescName;
	private String outputChannelName;

	public DummyConcreteHandler() {

	}

	int count = 0;

	@Override
	public Status process() throws HandlerException {
		// if (actionEvent == null)
		// return null;
		// actionEvent.getHeaders().put(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC,
		// inputSrcDescName);
		// actionEvent.getHeaders().put("channel", outputChannelName);
		logger.debug("processing handler",
				"_message=\"processing dummy handler\" handler_name={} this={} outputChannel={}", getName(), this,
				getOutputChannel());
		ActionEvent newActionEvent = new ActionEvent();
		newActionEvent.setBody("body".getBytes(Charset.defaultCharset()));
		if (getOutputChannel() != null) {
			getOutputChannel().put(newActionEvent);
		}
		count++;
		if (count >= 3) {
			return null;
		}

		// if (inputChannelMap !=null) {
		//
		// }
		return newActionEvent.getStatus();
		// throw new NotImplementedException();
		// this.state = State.RUNNING;
		// return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void build() throws AdaptorConfigurationException {
		super.build();

		Entry<String, String> srcDesc = (Entry<String, String>) getPropertyMap().get(SourceConfigConstants.SRC_DESC);
		if (srcDesc != null) {
			inputSrcDescName = srcDesc.getKey();
			outputChannelName = srcDesc.getValue();
		}

		// logger
		logger.debug("building handler", "handler_name={} properties={}", getName(), getPropertyMap());
		Set<String> keys = getPropertyMap().keySet();
		for (String key : keys) {
			logger.debug("building handler", "handler_name=\"{}\" key=\"{}\" value.class=\"{}\" value=\"{}\"",
					getName(), key, getPropertyMap().get(key).getClass(), getPropertyMap().get(key));
			// logger.debug("building handler", "handler_name=\"{}\" key=\"{}\"
			// value.class=\"{}\"", getName(), key,
			// getPropertyMap().get(key));
		}
	}

	protected void submitToChannel(ActionEvent actionEvent) {

	}
}
