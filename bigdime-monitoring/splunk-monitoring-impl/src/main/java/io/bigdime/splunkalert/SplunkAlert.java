/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.splunkalert;

import io.bigdime.alert.Alert;

/**
 * SplunkAlert object encapsulates all the fields which are very specific to Splunk.
 * 
 * @author samurthy
 * 
 */

public class SplunkAlert extends Alert {
	/**
	 * Name of the source defined on the splunk side.
	 */
	private String sourceType;
	/**
	 * Name of the host server where the alert was logged.
	 */
	private String host;
	/**
	 * Interpretation is a simplified/easy to understand version of alert
	 * details. This field would be a regex implemetation on splunk end raw
	 * data.
	 */
	private String interpretation;
	/**
	 * Raw data captured as a part of the exception.This would have the
	 * completed details of the Exception
	 */
	private String _raw;

	/**
	 * Gets the name of the source
	 * 
	 * @return
	 */
	public String getSourceType() {
		return sourceType;
	}

	/**
	 * Sets the name of the source
	 * 
	 * @param sourcetype
	 */
	public void setSourceType(String sourceType) {
		this.sourceType = sourceType;
	}

	/**
	 * Gets the name of the host
	 * 
	 * @return
	 */
	public String getHost() {
		return host;
	}

	/**
	 * Sets the name of the host
	 * 
	 * @param host
	 */
	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * Gets the interpreted regex value
	 * 
	 * @return
	 */
	public String getInterpretation() {
		return interpretation;
	}

	/**
	 * 
	 * @param interpretation
	 */
	public void setInterpretation(String interpretation) {
		this.interpretation = interpretation;
	}

	/**
	 * Gets the raw alert data
	 * 
	 * @return
	 */
	public String get_raw() {
		return _raw;
	}

	/**
	 * Sets the raw alert data
	 * 
	 * @param _raw
	 */
	public void set_raw(String _raw) {
		this._raw = _raw;
	}

}
