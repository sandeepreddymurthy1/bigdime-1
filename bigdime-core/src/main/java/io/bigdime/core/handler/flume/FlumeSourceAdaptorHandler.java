/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler.flume;

import java.util.Map;

import org.apache.flume.EventDeliveryException;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.PollableSource;
import org.apache.flume.PollableSource.Status;
import org.apache.flume.channel.ChannelProcessor;

import io.bigdime.core.Handler;
import io.bigdime.core.HandlerException;
import io.bigdime.core.handler.AbstractHandler;

//import io.bigdime.commons.alert.Logger.ALERT_TYPE;

/**
 * This class can be used to obtain a Handler instance for a given Flume
 * PollableSource or EventDrivenSource instance.
 *
 * @author Neeraj Jain
 *
 */
public class FlumeSourceAdaptorHandler extends AbstractHandler {
	private final Handler handler;

	// private final Logger logger;
	// private String adaptorSource;
//	private static final ThreadLocal<HandlerContext<FlumeSourceContext>> context = new ThreadLocal<HandlerContext<FlumeSourceContext>>();

	public FlumeSourceAdaptorHandler(EventDrivenSource source) throws SourceTypeNotSupportedException {
		// final EventDrivenSource eventDrivenSource = source;

		throw new SourceTypeNotSupportedException();
	}

	/**
	 * JMS, Kafka sources can be supported using this method
	 *
	 * @param source
	 * @return
	 * @throws SourceTypeNotSupportedException
	 */
	public FlumeSourceAdaptorHandler(PollableSource source) throws SourceTypeNotSupportedException {
		final PollableSource pollableSource = source;
		final org.apache.flume.ChannelSelector selector = null;

		/*
		 * This could be a io.bigdime.core.channel.DataChannelProcessor or
		 * org.apache.flume.channel.ChannelProcessor, based on configuration.
		 */
		final ChannelProcessor channelProcessor = null;
		// new io.bigdime.core.channel.DataChannelProcessor(
		// selector);
		// final DataChannelProcessor flumeChannelProcessor = new
		// DataChannelProcessor(
		// selector);

		final class Handler0 extends AbstractHandler {
			@Override
			public io.bigdime.core.ActionEvent.Status process() throws HandlerException {
				try {
					/*
					 * Set DataChannelProcessor on PollableSource, we dont want
					 * org.apache.flume.channel.ChannelProcessor
					 */
					pollableSource.setChannelProcessor(channelProcessor);
					/*
					 * Invoke process method on org.apache.flume.PollableSource
					 * and get Status. The process method submits data to a
					 * channel processor. If the
					 * org.apache.flume.channel.ChannelProcessor is used, the
					 * data is submitted to DataChannel directly after going
					 * through Flume interceptors. If
					 * io.bigdime.core.channel.DataChannelProcessor is used, the
					 * chain of handlers will be resumed.
					 */
					Status status = pollableSource.process();
//					context.get().get().getActionEvents();

					// HandlerContext.getData
					// Put this data in ActionEvent
				} catch (EventDeliveryException e) {
					// alert.error(adaptorSource,
					// ALERT_TYPE.INGESTION_FAILED_VALIDATION_ERROR,
					// e.toString());
					// alert.error(adaptorSource, MESSAGE_TYPE.INGESTION_FAILED,
					// CAUSE.VALIDATION_ERROR, e.toString());
				}
				return null;
			}

			@Override
			public void setPropertyMap(Map<String, Object> propertyMap) {
				// TODO Auto-generated method stub

			}

			@Override
			public void build() {
				// TODO Auto-generated method stub

			}

		}
		Handler handler0 = new Handler0();

		this.handler = handler0;
		throw new SourceTypeNotSupportedException();
	}

	// private Handler handler0(final PollableSource pollableSource) {
	// Handler handler = new AbstractHandler() {
	//
	// @Override
	// public ActionEvent process(ActionEvent actionEvent)
	// throws HandlerException, InterruptedException {
	// try {
	//
	// Status status = pollableSource.process();
	//
	// // HandlerContext.getData
	// // Put this data in ActionEvent
	// } catch (EventDeliveryException e) {
	// // alert.error(adaptorSource,
	// // ALERT_TYPE.INGESTION_FAILED_VALIDATION_ERROR,
	// // e.toString());
	// // alert.error(adaptorSource, MESSAGE_TYPE.INGESTION_FAILED,
	// // CAUSE.VALIDATION_ERROR, e.toString());
	// }
	// return null;
	// }
	// };
	// }
	// }
	@Override
	public io.bigdime.core.ActionEvent.Status process() throws HandlerException {
		return handler.process();
		// TODO Auto-generated method stub
//		return null;
	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub

	}

	@Override
	public void setPropertyMap(Map<String, Object> propertyMap) {
		// TODO Auto-generated method stub

	}

	@Override
	public void build() {
		// TODO Auto-generated method stub

	}
}
