package io.bigdime.validation.common;


import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.config.AdaptorConfig;

import org.apache.commons.lang.StringUtils;

public class AbstractValidator {
	
	private static final Logger logger = LoggerFactory.getLogger(AbstractValidator.class);
	
	/**
	 * This method will check provided parameters if null or empty or not
	 * 
	 * @param key
	 * @param value
	 * 
	 */
	public void checkNullStrings(String key, String value) {
		if (StringUtils.isBlank(value)) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
					"Checking Null/Empty for provided argument: " + key, " {} is null/empty", key);
			throw new IllegalArgumentException();
		}
	}	
}
