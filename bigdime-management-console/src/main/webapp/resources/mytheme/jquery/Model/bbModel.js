/*
 * Copyright (C) 2015 Stubhub.
 */
var model = Backbone.Model.extend({});
var radioButtonModel = Backbone.Model.extend({});
var tableRows; 
var initialload = [];
var jqxtreedata = [];
var initialloaddata = [];
var sourcedata =null;
var devHost='';
var qaHost='';
var prodHost='';
var devPort='';
var qaPort='';
var prodPort='';
var jqxtree='';  
function getjqxTree(selectedDropDownBox) { 
	  getApplicationProperties();
	$.each(pageSource, function(keys, values) {
		if (values.env.toLowerCase() == selected.env.toLowerCase()) {
			$.get(jqxtree, function(data) { 
				setJqTree(data);			 
			});
		}
	});
}
function getApplicationProperties() {
$.each(pageSource, function(keys, values) {
	 $.getJSON('propertyService/applicationproperties', function(data) { 	
		 devHost=data.devHost;
		 qaHost=data.qaHost;
		 prodHost=data.prodHost;
		 devPort=data.devPort;
		 qaPort=data.qaPort;
		 prodPort=data.prodPort;
		 
		  if(selected.env.toLowerCase()=='dev') {  	   
    	   jqxtree=data.devHost + data.devPort + alertdata
		  }
	       if(selected.env.toLowerCase()=='qa'){	   
    	   jqxtree=data.qaHost + data.qaPort + alertdata
		  }
          if(selected.env.toLowerCase()=='prod') { 	   
    	   jqxtree=data.prodHost + data.prodPort + alertdata
		 }
  		$.get(jqxtree, function(data) { 
			setJqTree(data);			 
		});
           
      });
   });
}

function setInitialdisplay() {
	for (var i = 0; i < pageSource.length; i++) {
		if (pageSource[i].env.toUpperCase() == selected.env.toUpperCase()) {
			if (typeof (pageSource[i][selected.tree.toLowerCase() + "url"]) == "object") {			
				var initialsource = [ {"datasource" : []} ];
				var totallength = Object.keys(pageSource[i][selected.tree.toLowerCase()+ "url"]).length;
				var i,j = 0;
				var tmpjsondata=null;
				var alertcount=0;
				$.each(pageSource[i][selected.tree.toLowerCase()+ "url"],
								function(keys, values) {
									$.get(values,function(jsondata) {
										tmpjsondata=jsondata;
										for(i=0;i<tmpjsondata.length;i++){
											alertcount=alertcount+tmpjsondata[i].raisedAlerts.length
										}
														initialsource[0].datasource
																.push({
																	"datasource" : keys,
																	"alerts" : alertcount
																})
														j++;alertcount=0;i=0;
														
														if (j == totallength) {
															var detailsTableInitalLoad = new detailsTableViewInitalLoad(
																	{
																		collection : initialsource
																	});
															detailsTableInitalLoad
																	.render();
														}
													})

								})

			} else {
				collection = Backbone.Collection.extend({
					url : pageSource[i][selected.tree.toLowerCase() + "url"],
					model : radioButtonModel,
					parse : function(response) {
						return response;
					}
				});
				initialsource = new collection;
				initialsource
						.fetch({
							success : function(values) {
								var detailsTableInitalLoad = new detailsTableViewInitalLoad(
										{
											collection : initialsource.toJSON()
										});
								detailsTableInitalLoad.render();
							}
						});
			}
			break;
		}
	}
}
function getData(event) {
	if (selected.tree == "" || selected.tree == null
			|| selected.tree == undefined || selected.tree=="Datasource") {
		event.preventDefault();
		return false;
	}
	var host;
	if(selected.env.toUpperCase().trim()=='PROD'){
		host=prodHost+prodPort;
	}else if(selected.env.toUpperCase().trim()=='DEV'){
	   host=devHost+devPort;	
	}else if(selected.env.toUpperCase().trim()=='QA'){
	   host=qaHost+qaPort;	
	}
	var getURLSFrom=[{
	type:selected.tree,
	env : selected.env,
	alertName:selected.tree,
	AlertUrl :host+context+selected.tree
    }];
	switch (parseInt(selected.tab)) {
	case 0:
		getAlertDetails(getURLSFrom[0].AlertUrl);
		break;
	}
}

function getAlertDetails(urlsToFetch) {
	collection = Backbone.Collection.extend({
		url : urlsToFetch,
		model : model,
		parse : function(response) {
			return response;
		}
	});
	source = new collection;
	source.fetch({
		success : function(values) {
			var AlertsTable = new Alertview({
				collection : source
			});
			AlertsTable.render();
		}
	});

}