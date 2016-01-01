/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.impl.biz.dao;


import io.bigdime.splunkalert.SplunkAlert;

import java.util.List;

/**
 * A pojo encapsulating list of a splunk alerts. 
 * @author Sandeep Reddy,Murthy
 *
 */

public class SplunkAlertData {

	
	List<SplunkAlert> raisedAlerts ;
/**
 * @return List of alerts to the caller
 */
	public List<SplunkAlert> getRaisedAlerts() {
		return raisedAlerts;
	}
/**
 * Sets list of alerts in the AlertData
 * @param raisedAlerts
 */
	public void setRaisedAlerts(List<SplunkAlert> raisedAlerts) {
		this.raisedAlerts = raisedAlerts;
	}
	

}
