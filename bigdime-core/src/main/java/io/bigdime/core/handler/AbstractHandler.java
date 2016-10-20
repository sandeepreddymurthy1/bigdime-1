/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.DataChannel;
import io.bigdime.core.Handler;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InputDescriptor;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.config.AdaptorConfigConstants.HandlerConfigConstants;
import io.bigdime.core.config.AdaptorConfigConstants.SourceConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore.Status;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;

/**
 * Abstract implementation of Handler.
 * 
 * @author Neeraj Jain
 *
 */

public abstract class AbstractHandler implements Handler {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(AbstractHandler.class));
	private final String id = UUID.randomUUID().toString();
	private State state;
	/**
	 * Name of this handler.
	 */
	private String name;
	private int index;

	/**
	 * Invocation count for this handler.
	 */
	private long invocationCount;

	/**
	 * Properties set for this handler.
	 */
	private Map<String, Object> propertyMap;

	/**
	 * Map from input to outputChannel, defines which outputChannel the data from an input should be sent to.
	 * @formatter:off
	 * e.g. "outputChannel-map" : "input1:channel1, input2:channel2",
	 * means input1's data should be sent to channel1 and input2's data should be sent to channel2.
	 * @formatter:on
	 */
	private DataChannel outputChannel;

	private String[] getInputChannelArray(String channelMapValue) {
		return channelMapValue.split(","); // spilt "input1:channel1,
											// input2:channel2"
	}

	@Override
	public void build() throws AdaptorConfigurationException {
		logger.info("building handler", "handler_index=\"{}\" handler_name=\"{}\" properties=\"{}\"", getIndex(),
				getName(), getPropertyMap());
		@SuppressWarnings("unchecked")
		Entry<String, String> srcDesc = (Entry<String, String>) getPropertyMap().get(SourceConfigConstants.SRC_DESC);
		logger.info("building handler", "handler_name=\"{}\" \"src_desc\"=\"{}\" handler=\"{}\"", getName(), srcDesc,
				this.getId());
		if (srcDesc != null) {
			String srcDescInputName = srcDesc.getValue();
			if (getPropertyMap().containsKey(HandlerConfigConstants.CHANNEL_MAP)) {
				String channelMapValue = getPropertyMap().get(HandlerConfigConstants.CHANNEL_MAP).toString();
				logger.debug("building handler", "handler_name=\"{}\" channel_map=\"{}\"", getName(), channelMapValue);
				String[] inputChannelArray = getInputChannelArray(channelMapValue);

				final Map<String, DataChannel> channelMap = AdaptorConfig.getInstance().getAdaptorContext()
						.getChannelMap();

				for (String inputChannelValue : inputChannelArray) {
					String[] inputChannelValuesMap = inputChannelValue.split(":");
					if (inputChannelValuesMap.length != 2) {
						throw new AdaptorConfigurationException("value must be in input:channel format");
					}
					String inputName = inputChannelValuesMap[0].trim();
					if (!inputName.equals(srcDescInputName)) {
						continue;
					}
					setOutputChannel(channelMap.get(inputChannelValuesMap[1].trim()));
					if (getOutputChannel() == null) {
						throw new AdaptorConfigurationException(
								"invalid value of outputChannel, outputChannel with name=" + inputName + " not found");
					}
					break;
				}

				if (outputChannel == null) {
					throw new AdaptorConfigurationException("no channel mapped for input=" + srcDescInputName);
				}
				logger.debug("building handler", "handler_name=\"{}\" src_desc=\"{}\" outputChannel=\"{}\"", getName(),
						srcDescInputName, outputChannel.getName());
			}
		}
	}

	@Override
	public String getId() {
		return id;
	}

	protected boolean setState(final State state) {
		if (this.state == state) {
			return false;
		}
		this.state = state;
		return true;
	}

	@Override
	public State getState() {
		return state;
	}

	@Override
	public void shutdown() {
		state = State.TERMINATED;
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

	protected Map<String, Object> getPropertyMap() {
		return propertyMap;
	}

	public DataChannel getOutputChannel() {
		return outputChannel;
	}

	protected void setOutputChannel(DataChannel outputChannel) {
		this.outputChannel = outputChannel;
	}

	/**
	 * 
	 * @param runtimeInfoStore
	 *            store to get from and save to runtime information
	 * @param entityName
	 *            entity that the runtime information belongs to
	 * @param availableDescriptors
	 *            list of descriptors(files, tables etc) that are available to
	 *            this handler
	 * @param inputDescriptor
	 *            descriptor
	 * @return
	 * @throws RuntimeInfoStoreException
	 */
	protected <T> T getNextDescriptorToProcess(final RuntimeInfoStore<RuntimeInfo> runtimeInfoStore,
			final String entityName, List<T> availableDescriptors, final InputDescriptor<T> inputDescriptor)
					throws RuntimeInfoStoreException {
		final List<RuntimeInfo> runtimeInfos = runtimeInfoStore.getAll(AdaptorConfig.getInstance().getName(),
				entityName);
		logger.debug(getHandlerPhase(), "runtimeInfos=\"{}\"", runtimeInfos);

		final RuntimeInfo runtimeInfo = runtimeInfoStore.getLatest(AdaptorConfig.getInstance().getName(), entityName);
		logger.debug(getHandlerPhase(), "latestRuntimeInfo=\"{}\"", runtimeInfo);
		T nextDescriptorToProcess = null;
		if (runtimeInfo == null) {
			nextDescriptorToProcess = availableDescriptors.get(0);
		} else {
			logger.debug(getHandlerPhase(), "handler_id=\"{}\" latestRuntimeInfo=\"{}\"", getId(), runtimeInfo);
			String lastRuntimeInfoDescriptor = runtimeInfo.getInputDescriptor();
			nextDescriptorToProcess = inputDescriptor.getNext(availableDescriptors, lastRuntimeInfoDescriptor);
			logger.debug(getHandlerPhase(), "computed nextDescriptorToProcess=\"{}\"", nextDescriptorToProcess);
		}
		return nextDescriptorToProcess;
	}

	protected RuntimeInfo getOneQueuedRuntimeInfo(final RuntimeInfoStore<RuntimeInfo> runtimeInfoStore,
			final String entityName) throws RuntimeInfoStoreException {
		final List<RuntimeInfo> runtimeInfos = runtimeInfoStore.getAll(AdaptorConfig.getInstance().getName(),
				entityName, RuntimeInfoStore.Status.QUEUED);
		logger.debug(getHandlerPhase(), "queued_runtimeInfos=\"{}\"", runtimeInfos);
		if (runtimeInfos != null && !runtimeInfos.isEmpty()) {
			return runtimeInfos.get(0);
		}
		return null;
	}

	protected List<RuntimeInfo> getAllStartedRuntimeInfos(final RuntimeInfoStore<RuntimeInfo> runtimeInfoStore,
			final String entityName) throws RuntimeInfoStoreException {
		final List<RuntimeInfo> runtimeInfos = runtimeInfoStore.getAll(AdaptorConfig.getInstance().getName(),
				entityName, RuntimeInfoStore.Status.STARTED);
		logger.debug(getHandlerPhase(), "started_runtimeInfos=\"{}\"", runtimeInfos);
		return runtimeInfos;
	}

	/**
	 * 
	 * Update RuntimeInfoStore with the status as Started.
	 * 
	 * @param runtimeInfoStore
	 * @param entityName
	 * @param inputDescriptor
	 * @return true if the update was successful, false otherwise
	 * @throws RuntimeInfoStoreException
	 */
	protected <T> boolean addRuntimeInfo(final RuntimeInfoStore<RuntimeInfo> runtimeInfoStore, final String entityName,
			final String inputDescriptor) throws RuntimeInfoStoreException {
		return updateRuntimeInfo(runtimeInfoStore, entityName, inputDescriptor, RuntimeInfoStore.Status.STARTED);
	}

	protected <T> boolean queueRuntimeInfo(final RuntimeInfoStore<RuntimeInfo> runtimeInfoStore,
			final String entityName, final String inputDescriptor) throws RuntimeInfoStoreException {
		if (runtimeInfoStore.get(AdaptorConfig.getInstance().getName(), entityName, inputDescriptor) == null) {
			logger.info(getHandlerPhase(), "queueing adaptorName=\"{}\" entityName={} inputDescriptor={}",
					AdaptorConfig.getInstance().getName(), entityName, inputDescriptor);
			return updateRuntimeInfo(runtimeInfoStore, entityName, inputDescriptor, RuntimeInfoStore.Status.QUEUED);
		} else {
			logger.debug(getHandlerPhase(), "already in progress, adaptorName=\"{}\" entityName={} inputDescriptor={}",
					AdaptorConfig.getInstance().getName(), entityName, inputDescriptor);

		}
		return false;
	}

	protected <T> boolean updateRuntimeInfoToStoreAfterValidation(final RuntimeInfoStore<RuntimeInfo> runtimeInfoStore,
			boolean validationPassed, ActionEvent actionEvent) throws RuntimeInfoStoreException {
		
		String entityName = actionEvent.getHeaders().get(ActionEventHeaderConstants.ENTITY_NAME);	
		String inputDescriptor = actionEvent.getHeaders().get(ActionEventHeaderConstants.INPUT_DESCRIPTOR);
		Map<String, String> properties = actionEvent.getHeaders();
		if (validationPassed) {
			return updateRuntimeInfo(runtimeInfoStore, entityName, inputDescriptor, Status.VALIDATED, properties);
		} else {
			return updateRuntimeInfo(runtimeInfoStore, entityName, inputDescriptor, Status.FAILED, properties);
		}
	}

	protected <T> boolean updateRuntimeInfo(final RuntimeInfoStore<RuntimeInfo> runtimeInfoStore,
			final String entityName, final String inputDescriptor, RuntimeInfoStore.Status status)
					throws RuntimeInfoStoreException {
		return updateRuntimeInfo(runtimeInfoStore, entityName, inputDescriptor, status, null);
	}

	protected <T> boolean updateRuntimeInfo(final RuntimeInfoStore<RuntimeInfo> runtimeInfoStore,
			final String entityName, final String inputDescriptor, RuntimeInfoStore.Status status,
			Map<String, String> properties) throws RuntimeInfoStoreException {
		RuntimeInfo startingRuntimeInfo = new RuntimeInfo();
		startingRuntimeInfo.setAdaptorName(AdaptorConfig.getInstance().getName());
		startingRuntimeInfo.setEntityName(entityName);
		startingRuntimeInfo.setInputDescriptor(inputDescriptor);
		startingRuntimeInfo.setStatus(status);
		startingRuntimeInfo.setProperties(properties);
		logger.debug(getHandlerPhase(), "updating runtime info store, calling put");
		return runtimeInfoStore.put(startingRuntimeInfo);
	}

	/**
	 * Get the context from ThreadLocal.
	 * 
	 * @return
	 */
	protected HandlerContext getHandlerContext() {
		return HandlerContext.get();
	}

	/**
	 * Get the journal for this handler.
	 * 
	 * @return
	 */
	// protected Object getJournal() {
	// return getHandlerContext().getJournal(getId());
	// }

	@SuppressWarnings("unchecked")
	protected <T extends HandlerJournal> T getJournal(Class<T> clazz) throws HandlerException {
		return (T) getHandlerContext().getJournal(getId());
	}

	protected <T extends HandlerJournal> T getNonNullJournal(Class<T> clazz) throws HandlerException {
		// return (T) getHandlerContext().getJournal(getId());
		T journal = getJournal(clazz);
		if (journal == null) {
			try {
				journal = clazz.newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new HandlerException(ex);
			}
		}
		getHandlerContext().setJournal(getId(), journal);
		return journal;
	}

	/**
	 * Set the journal for this handler.
	 * 
	 * @param journal
	 */
	protected void setJournal(HandlerJournal journal) {
		getHandlerContext().setJournal(getId(), journal);
	}

	protected String getHandlerPhase() {
		return "processing abstractHandler";
	}

	/**
	 * @param outputEvent
	 */
	protected void processChannelSubmission(final ActionEvent outputEvent) {
		logger.debug(getHandlerPhase(), "checking channel submission, output_channel=\"{}\"", getOutputChannel());
		if (getOutputChannel() != null) {
			logger.debug(getHandlerPhase(), "submitting to channel");
			getOutputChannel().put(outputEvent);
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
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
		AbstractHandler other = (AbstractHandler) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

	@Override
	public void handleException() {
		// default implementation does nothing.
	}

	@Override
	public void setIndex(final int index) {
		this.index = index;
	}

	@Override
	public int getIndex() {
		return index;
	}

	public long getInvocationCount() {
		return invocationCount;
	}

	public void incrementInvocationCount() {
		this.invocationCount++;
	}
}