/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.source;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.Handler;
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.Source;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.config.HandlerConfig;
import io.bigdime.core.config.SourceConfig;
import io.bigdime.core.handler.HandlerFactory;

/**
 * Factory to create DataSource object for the given {@link SourceConfig}
 * object.
 *
 * @author Neeraj Jain
 *
 */
@Component
public final class DataSourceFactory {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(DataSourceFactory.class));
	private static final String SRC_DESC = "src-desc";
	@Autowired
	private HandlerFactory handlerFactory;

	public Collection<Source> getDataSource(SourceConfig sourceConfig) throws AdaptorConfigurationException {

		if (sourceConfig == null) {
			throw new AdaptorConfigurationException("sourceConfig can't be null");
		}
		Collection<Source> dataSources = new HashSet<>();
		int sourceInstance = 0;
		Map<String, String> descInputEntry = SrcDescReader.getDescriptorEntryMap(sourceConfig);
		for (Entry<String, String> entry : descInputEntry.entrySet()) {
			sourceInstance++;
			final DataSource dataSource = new DataSource(getHandlers(sourceConfig.getHandlerConfigs(), entry),
					sourceConfig.getName() + "-" + sourceInstance);
			dataSource.setDescription(sourceConfig.getDescription());
			dataSources.add(dataSource);
		}
		return dataSources;
	}

	private LinkedHashSet<Handler> getHandlers(final Set<HandlerConfig> handlerConfigs,
			Entry<String, String> descInputEntry) throws AdaptorConfigurationException {
		final LinkedHashSet<Handler> handlers = new LinkedHashSet<>();
		// final LinkedHashSet<HandlerConfig> handlerConfigs =
		// sourceConfig.getHandlerConfigs();
		int index = 0;
		for (HandlerConfig handlerConfig : handlerConfigs) {
			logger.debug("building source", "adding handler, handler_name=\"{}\"", handlerConfig.getName());
			handlerConfig.getHandlerProperties().put(SRC_DESC, descInputEntry);
			final Handler handler = handlerFactory.getHandler(handlerConfig);
			handler.setIndex(index);
			handlers.add(handler);
			index++;
		}
		return handlers;
	}

	private static class SrcDescReader {
		/**
		 * @formatter:off
		 * Case 1: If the src-desc is defined as:
		 * "src-desc": {
		 *   "input1" : "topic1:par1,par2,par3,"
		 * }, then
		 * a map with following entries will be returned.
		 * {"topic1:par1", "input1"
		 * "topic1:par2", "input1"
		 * "topic1:par3", "input1"}
		 * 
		 * Case 2: If the src-desc is defined as:
		 * "src-desc": {
		 *   "input1" : "topic1:par1,par2,par3,"
		 *   "input2" : "topic1:par4",
		 *   "input3" : "topic2:par1"
		 * },
		 * a map with following entries will be returned.
		 * {"topic1:par1", "input1"
		 * "topic1:par2", "input1"
		 * "topic1:par3", "input1"
		 * "topic1:par4", "input2"
		 * "topic2:par1", "input3"
		 * }
		 * 
		 * Case 3: If the src-desc is defined as:
		 * "src-desc": {
		 *   "input1" : "topic1:"
		 * },
		 * a map with following entries will be returned.
		 * {"topic1", "input1"
		 * }
		 * 
		 * @formatter:on
		 * @param sourceConfig
		 * @return
		 * @throws InvalidValueConfigurationException 
		 */
		private static Map<String, String> getDescriptorEntryMap(SourceConfig sourceConfig)
				throws InvalidValueConfigurationException {
			Map<String, String> descInputEntry = new HashMap<>();
			Map<String, String> srcDesc = sourceConfig.getSrcDesc();
			for (Entry<String, String> input : srcDesc.entrySet()) {
				String descriptors = input.getValue(); // srcDesc.get(input);
				String[] inputArray = descriptors.split(":");
				logger.debug("parsing src_desc->input", "1:inputArray=\"{}\"", inputArray.length);

				String desc = "";
				if (inputArray.length == 2) {
					final String part1 = inputArray[0].trim();
					final String part2Str = inputArray[1];
					final String[] part2Array = part2Str.split(",");
					for (String part2 : part2Array) {
						logger.debug("parsing src_desc->input", "part1=\"{}\" part2=\"{}\"", part1, part2);
						part2 = part2.trim();
						// if (StringUtils.isBlank(part1) ||
						// StringUtils.isBlank(part2))
						// throw new InvalidValueConfigurationException(
						// "invalid value specified for " + input.getKey());
						desc = part1 + ":" + part2;
						logger.debug("putting srcDesc", "desc=\"{}\" input=\"{}\"", desc, input.getKey());
						descInputEntry.put(desc, input.getKey());
						// inputs.put(part1 + ":" + part2, key);
					}
				} else if (inputArray.length == 1) {
					final String part2Str = inputArray[0];
					final String[] part2Array = part2Str.split(",");
					for (final String part2 : part2Array) {
						logger.debug("parsing src_desc->input", "part2=\"{}\"", part2);
						// inputs.put(part2, key);
						desc = part2.trim();
						if (StringUtils.isBlank(desc))
							throw new InvalidValueConfigurationException(
									"invalid value(blank) specified for " + input.getKey());
						logger.debug("putting srcDesc", "desc=\"{}\" input=\"{}\"", desc, input);
						logger.debug("putting srcDesc", "desc=\"{}\" key=\"{}\" input=\"{}\"", desc,
								input.getKey().trim(), input);
						descInputEntry.put(desc, input.getKey().trim());
					}
				} else {
					logger.warn("parsing src_desc->input", "input=\"{}\"", input);
					throw new InvalidValueConfigurationException("invalid value specified for " + input.getKey());

				}
			}
			return descInputEntry;
		}
	}
}
