/*
 * Copyright (C) 2015 Stubhub.
 */
var Alertview = Backbone.View
		.extend({
			el : $('#alertsTable'), // 
			initialize : function() {
				_.bindAll(this, 'render'); // fixes loss of context for 'this'
				// within methods
			},
			render : function(eventName) {
				$("#alertsTable tr:gt(0)").remove();
				this.$el
			        .append("<tr><td colspan='6'></td></tr><tr><th>Source </th><th>Severity<th>Time</th><th>Status</th><th>Message</th><th>Detailed Message</th></tr>"); // Append
							
				var searchHost = [];
				var searchTime = []
				var searchSource = [];
				var searchEventType = [];
				this.collection
						.each(function(model) {
							var raisedAlertslength = model.get('raisedAlerts').length;
							for (i =raisedAlertslength-1;i>=0; i--) {

								$("#alertsTable")
										.append(
												"<tr><td>"
														+ model
														        .get('raisedAlerts')[i].applicationName
														+ "</td><td>"
														+ model
														        .get('raisedAlerts')[i].logLevel+"("
														+model
												                .get('raisedAlerts')[i].severity+")"
														+ "</td><td>"
														+ new Date(model
														        .get('raisedAlerts')[i].dateTime).toGMTString()
														+ "</td><td>"														
														+ model
																.get('raisedAlerts')[i].comment
														+ "</td><td>"														
														+ model
														        .get('raisedAlerts')[i].messageContext
														+ "</td><td>"														
														+ model
														        .get('raisedAlerts')[i].message
														+ "</td></tr>");
							}
						});

			}
		});
var delay = (function(){
	  var timer = 0;
	  return function(callback, ms){
	    clearTimeout (timer);
	    timer = setTimeout(callback, ms);
	  };
	})();
$("#searchAlertSourceID").keyup(
			function() {delay(function(){
				setAlertTable([ $("#searchAlertSourceID").val()]);
	           }, 500 );
			});

//$("#searchAlertSourceID").keyup(
//		function() {
//			setAlertTable([ $("#searchAlertSourceID").val()]);
//
//		});

$("#searchAlertHostID").keyup(
		function() {
			setAlertTable([ $("#searchAlertHostID").val()]);

		});

function setAlertTable(searchAlertArray) {
	searchAlertArrayLength = searchAlertArray.length;
	var searchAlertLogic = new Array($("#alertsTable tr:gt(1)").length);
	for (i = 0; i < searchAlertLogic.length; i++) {
		searchAlertLogic[i] = new Array(searchAlertArrayLength);
	}
	for (var i = 0; i < searchAlertArrayLength; i++) {
		var j = 0;
		$("#alertsTable tr:gt(1)")
				.each(
						function() {
							if (i == 2) {
								if (searchAlertArray[i] == null
										|| searchAlertArray[i] == undefined
										|| searchAlertArray[i] == '') {
									searchAlertLogic[j][i] = true;
								} else {
									var newselecteddate = moment(new Date(
											searchAlertArray[i]));
									momentNewSelectedDate = moment(
											newselecteddate).format("x");
									var actualdate = $(this).find("td:eq(2)")
											.text();
									momentActualDate = moment(actualdate)
											.format("x");
									if ((momentActualDate - momentNewSelectedDate) >= 0) {
										searchAlertLogic[j][i] = true;
									} else {
										searchAlertLogic[j][i] = false;
									}
								}
							} else if (i >= 3) {

								if (searchAlertArray[i] == null
										|| searchAlertArray[i] == undefined
										|| searchAlertArray[i] == '') {
									searchAlertLogic[j][i] = true;
								} else {
									var newselecteddate = moment(new Date(
											searchAlertArray[i]));
									momentNewSelectedDate = moment(
											newselecteddate).format("x");
									var actualdate = $(this).find("td:eq(2)")
											.text();
									momentActualDate = moment(actualdate)
											.format("x");
									if ((momentActualDate - momentNewSelectedDate) < 0) {
										searchAlertLogic[j][i] = true;
									} else {
										searchAlertLogic[j][i] = false;
									}
								}

							} else {
								var searchText;
								for(var k=0;k<6;k++){
									searchText=$(this).find("td:eq(" + k + ")").text().toUpperCase();
									if(searchText.indexOf(searchAlertArray[i]
									.toUpperCase()) != -1){
//									.toUpperCase()) != -1|| searchText == ''){
										searchAlertLogic[j][i] = true;
										break;
									} else {
										searchAlertLogic[j][i] = false;
									}
								}
//								var searchAlertText = $(this).find(
////										"td:eq(" + i + ")").text()
//										"td:eq(1)").text()
//										.toUpperCase();
//								var searchTimeText = $(this).find(
//										"td:eq(2)").text()
//										.toUpperCase();
//								if ((searchAlertText.indexOf(searchAlertArray[i]
//										.toUpperCase()) != -1
//										|| searchAlertText == '') ||
//										(searchTimeText.indexOf(searchAlertArray[i]
//										.toUpperCase()) != -1
//										|| searchAlertText == '')) {
								
//								if(true){
//									searchAlertLogic[j][i] = true;
//								} else {
//									searchAlertLogic[j][i] = false;
//								}
							}
							j++;
						})
	}

	for (var i = 0; i < searchAlertLogic.length; i++) {
		//    console.log(searchAlertLogic[i]);
		var output = true;
		for (var j = 0; j < searchAlertArrayLength; j++) {
			output = searchAlertLogic[i][j] && output;

		}

		if (output) {

			$("#alertsTable tr:eq(" + parseInt(i + 2) + ")").show();
		} else {
			$("#alertsTable tr:eq(" + parseInt(i + 2) + ")").hide();
		}
	}

}

var pressed ;
var start ;
var startX, startWidth;

$(function() {
    var pressed = false;
    var start = undefined;
});

$("#alertsTable").on("mousedown","th",function(e) {
    start = $(this);
    pressed = true;
    startX = e.pageX;
    startWidth = $(this).width();
    $(start).addClass("resizing");
});

$(document).mousemove(function(e) {
    if(pressed) {
        $(start).width(startWidth+(e.pageX-startX));
    }
});

$(document).mouseup(function() {
    if(pressed) {
        $(start).removeClass("refactoring");
        pressed = false;
    }
});

