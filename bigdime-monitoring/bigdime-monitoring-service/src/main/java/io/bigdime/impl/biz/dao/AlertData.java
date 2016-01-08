/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.impl.biz.dao;

import io.bigdime.alert.ManagedAlert;

import java.util.List;
/**
 * A pojo encapsulating list of a alerts. 
 * @author Sandeep Reddy,Murthy
 *
 */
public class AlertData {
	
	
	List<ManagedAlert> raisedAlerts;
	/**
	 * @return List of alerts to the caller
	 */

	public List<ManagedAlert> getRaisedAlerts() {
		return raisedAlerts;
	}
	
	/**
	 * Sets list of alerts in the AlertData
	 * @param raisedAlerts
	 */
	public void setRaisedAlerts(List<ManagedAlert> raisedAlerts) {
		this.raisedAlerts = raisedAlerts;
	}
	
	

}
