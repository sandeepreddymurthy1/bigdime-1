/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.event.client.utils;

import static io.bigdime.ha.event.manager.common.ZKEventManagerConstants.DASH;
import static io.bigdime.ha.event.manager.common.ZKEventManagerConstants.DOT;

import java.lang.management.ManagementFactory;

import com.google.common.base.Preconditions;

/**
 * 
 * @author mnamburi
 *
 */
public final class ZKEventManagerUtils {
	private ZKEventManagerUtils(){}
	/**
	 * ManagementFactory.getRuntimeMXBean().getName() return the ProcessID@IP_ADDRESS.
	 * We are extracting the process id from the string using @
	 * @return hostname-processid
	 */
	public static String getProcessID(){
		String ip_processID = null;
		String ids[] =  ManagementFactory.getRuntimeMXBean().getName().split("@");
		Preconditions.checkNotNull(ids[0]);
		Preconditions.checkNotNull(ids[1]);
		// Akka Process Name supports  only word characters (i.e. [a-zA-Z0-9] plus non-leading '-')
		//********************************
		if(ids[1].contains(DOT)){
			ip_processID = ids[1].replace(DOT,DASH) + DASH + ids[0];
		} else {
			ip_processID = ids[1] + DASH +ids[0];
		}
		return ip_processID;
	}
}
