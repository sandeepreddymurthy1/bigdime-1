/*
 * Copyright (C) 2015 Stubhub.
 */
var dontreloadLoadHistoryTab = false;
var totalalert;
var detailsTableView = Backbone.View
		.extend({
			el : $('#detailsTable'),
			initialize : function() {
				_.bindAll(this, 'render');
			},
			render : function(eventName) {
				$("#detailsTable tr").remove();
				searchRow = '<tr><td colspan="7"><input type="text" class="form-control searchSource" value="" placeholder="Search source" id="tbFeeds"></td></tr>';
				$("#detailsTable").append(searchRow);
				tableheader = '<tr><th>Name</th><th>HDFS Location</th><th>Hive Database</th><th>Hive Table</th><th>Access</th><th>Columns</th><th> Partition Columns</th></tr>';
				this.$el.append(tableheader);
				this.collection
						.each(
								function(model) {
									this.$el.append('<tr class="newRow"><td>'
													+ model.get('name')
													+ '</td><td>'
													+ model.get('hdfsLocation')
													+ '</td><td>'
													+ model.get('hiveDatabase')
													+ '</td><td>'
													+ model.get('hiveTable')
													+ '</td><td>'
													+ model.get('permissions')
													+ '</td><td><button  class="btn btn-default columnsshow">Show</button></td><td><button class="btn btn-default partitionshow">Show</button></td></tr>');
								}, this);

				var searchSourcesArray = [];
				$(this.collection.toJSON()).each(function(keys, values) {
					searchSourcesArray.push(values.name);
				});

				$('#tbFeeds')
						.autocomplete(
								{
									source : searchSourcesArray,
									minLength : 0,
									scroll : true,
									select : function(event, ui) {
										
										$("#detailsTable tr:gt(1)")
												.each(
														function() {
															if ($(this)
																	.hasClass(
																			"newRow")) {
																var firsttd = $(
																		this)
																		.children(
																				"td:eq(0)")
																		.text()
																		.toUpperCase();
																if (firsttd == ui.item.label
																		.toUpperCase()) {
																	$(this)
																			.show();
																} else {
																	$(this)
																			.hide()
																}
																$(this)
																		.find(
																				"button")
																		.text(
																				"Show")
															} else {
																$(this)
																		.remove();
															}
														});
										$("#statusTable tr:gt(0)").hide();
										dontreloadLoadHistoryTab = true;
										$("#statusTable tr:gt(0)")
												.each(
														function() {
															var firsttd = $(
																	this)
																	.children(
																			"td:eq(0)")
																	.text()
																	.toUpperCase();
															if (firsttd == ui.item.label
																	.toUpperCase()) {
																$(this).show();
															}
														});
									}
								})
						.focus(function() {
							$(this).autocomplete("search", "");
						})
						.keydown(
								function(e, ui) {
									if (e.keyCode === 13) {
										console.log($('#tbFeeds').val()
												.toUpperCase());
										$("#detailsTable tr:gt(1)")
												.each(
														function() {
															if ($(this)
																	.hasClass(
																			"newRow")) {
																var firsttd = $(
																		this)
																		.children(
																				"td:eq(0)")
																		.text()
																		.toUpperCase();
																if (firsttd
																		.indexOf($(
																				'#tbFeeds')
																				.val()
																				.toUpperCase()) > -1) {
																	$(this)
																			.show();
																} else {
																	$(this)
																			.hide()
																}
																$(this)
																		.find(
																				"button")
																		.text(
																				"Show")
															} else {
																$(this)
																		.remove();
															}
														});
										$("#statusTable tr:gt(0)").hide();
										dontreloadLoadHistoryTab = true; 
										$("#statusTable tr:gt(0)")
												.each(
														function() {
															var firsttd = $(
																	this)
																	.children(
																			"td:eq(0)")
																	.text()
																	.toUpperCase();
															if (firsttd
																	.indexOf($(
																			'#tbFeeds')
																			.val()
																			.toUpperCase()) > -1) {
																$(this).show();
															}
														});
									}
								});
			}
		});

var statusView = Backbone.View.extend({ 
	el : $('#statusTable'), 
	initialize : function() {
		_.bindAll(this, 'render'); 
	},
	render : function(eventName) {
		if (!dontreloadLoadHistoryTab) { 
			this.collection.each(function(model) {
				var missingdates = "";
				if (model.get('missingDates')) {
					var fromJSON = model.get('missingDates');
					var missingdatesLength = model.get('missingDates').length;
					for (var i = 0; i < missingdatesLength - 1; i++) {
						missingdates = missingdates + fromJSON[i] + ", ";
					}
					missingdates = missingdates
							+ fromJSON[missingdatesLength - 1] + ". ";
				}
				missingdates.slice(0, -1);
				if(missingdates=='undefined. ' || missingdates==''||missingdates==null){					
					missingdates="No Missing Dates";
				}
				$("#statusTable").append(
						'<tr><td>' + model.get('name') + '</td><td>'
								+ model.get('earliestDate') + '</td><td>'
								+ model.get('latestDate')
								+ '</td><td style="word-break: break-all">'
								+ missingdates + '</td></tr>');

			})
			dontreloadLoadHistoryTab = true; // resetting the data to false.
		}
	}

});

function displayPartitionColumns(name) {
	var getvalues = findSource(name, "partitionColumns");
	var partitontable = '<tr><th colspan="7"></th></tr><tr><th colspan="7">Partition Columns</th></tr><tr><td></td><td colspan="6"></td></tr><tr><th colspan="2">Name</th><th colspan="3">Type</th><th colspan="2">Comment</th></tr>';
	var rowcount = getvalues.length;
	for (i = 0; i < rowcount; i++) {
		partitontable = partitontable + '<tr><td colspan="2">'
				+ getvalues[i].name + '</td><td colspan="3">'
				+ getvalues[i].type + '</td></tr>';
	}
	$("#detailsTable").append(partitontable);
};

function displayColumns(name) {
	var getvalues = findSource(name, "columns");
	var partitontable = '<tr><th colspan="7"></th></tr><tr><th colspan="7"> Columns</th></tr><tr><td></td><td colspan="6"></td></tr><tr><th colspan="2">Name</th><th colspan="3">Type</th><th colspan="2">Comment</th></tr>';
	var rowcount = getvalues.length;
	for (i = 0; i < rowcount; i++) {
		partitontable = partitontable + '<tr><td colspan="2">'
				+ getvalues[i].name + '</td><td colspan="3">'
				+ getvalues[i].type + '</td></tr>';
	}
	$("#detailsTable").append(partitontable);
};
function findSource(name, arrayof) {
	var sourcedatas = sourcedata.toJSON();
	var sourceLength = sourcedatas.length;
	var sourceid = null;
	for (i = 0; i < sourceLength; i++) {
		console.log(sourcedatas[i].name );
		if (sourcedatas[i].name == name) {
			sourceid = i;
		}
	}
	return sourcedatas[sourceid][arrayof];
}
var detailsTableViewInitalLoad = Backbone.View.extend({ // Initial Load View
	el : $('#detailsTable'),
	initialize : function() { 
		_.bindAll(this, 'render');
	},
	render : function(eventName) {
		$("#detailsTable tr").remove();
		var tableheader = "<tr>";
		var populatedata = this.collection[0][selected.tree.toLowerCase()];
		$.each(populatedata[0], function(keys, values) {
			tableheader = tableheader
					+ '<th class="capitalize" data-reference="' + keys + '">'
					+ keys + '</th>';
		});
		tableheader = tableheader + "</tr>";
		$("#detailsTable").append(tableheader);
		var tabledata = '';
		totalalert = 0; // to calculate total alert
		$.each(populatedata, function(keys, values) {
			thLength = $("#detailsTable tr:eq(0) th").length;
			tabledata = tabledata + "<tr>";
			for (var i = 0; i < thLength; i++) {
				thvalue = $("#detailsTable tr:eq(0) th:eq(" + i + ")").attr(
						"data-reference");
				if (thvalue.toLowerCase() != "alerts") {
					tabledata = tabledata + '<td>' + values[thvalue] + '</td>';
				} else {
					numberClass = "";
					if (values[thvalue] != 0) {
						numberClass = "alert-number";
					}
					tabledata = tabledata
							+ '<td class="clickPopup" data-source="'
							+ values['datasource'] + '"><span class="'
							+ numberClass + '">' + values[thvalue]
							+ "</span> - alerts" + '</td>'
				}
			}
			tabledata = tabledata + "</tr>";
		})
		$("#detailsTable").append(tabledata);
	}
});

$(document).on("click", ".clickPopup", function() {
	 var tree = $('#jqxTree').jqxTree('getItems') 
	    var treelength = tree.length;
	   for(var i =0; i < treelength; i++){
	   if(tree[i].label.toLowerCase() == $(this).attr("data-source")){
	        setbreadcrumbs(tree[i]);
	       break;
	    }
	   };
	updateSelected('tree', $(this).attr("data-source")); 
	updateSelected('tab', 1); 
	$('#jqxTabs').jqxTabs({
		selectedItem : 1
	});
	getData();

})
