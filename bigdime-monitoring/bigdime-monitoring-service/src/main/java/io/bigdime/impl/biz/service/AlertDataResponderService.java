/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.impl.biz.service;


import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * AlertDataResponderService offers methods that can be used by the webservice's
 * end client to make calls for alert data. It also provides the path info for
 * calling these specific methods
 * 
 * @author Sandeep Reddy,Murthy
 * @version 1.0
 */

@Path("/")
public interface AlertDataResponderService {
	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	@Path("/splunkalerts")
	/**
	 * getSplunkAlerts method is used to make a call for data from the end webservice client.
	 * The data consists of list of splunk alerts represented in the form of a json
	 * @return Response object wrapping json containing the list of alerts 
	 * @deprecated this method would be deprecated from the final cut
	 */
	public Response getSplunkAlerts();

	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	@Path("/recentalerts/")
	/**
	 * getAlerts method is used to make a call for data from the end webservice client.
	 * The data consists of list of alerts represented in the form of a json
	 * @return Response object wrapping json containing the list of alerts 
	 */
	public Response getAlerts(@QueryParam("alertName") String alertName);

	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	@Path("/alerts/")
	/**
	 * getAlerts method is used to make a call for data from the end webservice client.
	 * The data consists of list of alerts represented in the form of a json
	 * @return Response object wrapping json containing the list of alerts 
	 */
	public Response getAlerts(@QueryParam("alertName") String alertName,
			@QueryParam("fromDate") long fromDate,
			@QueryParam("toDate") long toDate);
	
	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	@Path("/alertset/")
    /**
     * getListofAlerts would return all the alerts which are configured for monitoring
     */
	public Response getSetOfAlerts();
}
