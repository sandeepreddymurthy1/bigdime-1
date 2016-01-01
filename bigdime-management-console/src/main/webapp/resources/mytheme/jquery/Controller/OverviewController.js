/*
 * Copyright (C) 2015 Stubhub.
 */
var selected = {}; 
var jqxlabelsforArray; 
$(document).ready(function() {
	$(".radioURL select option:first").attr("selected", "selected"); 
	$('#jqxTabs').jqxTabs({
		width : '100%',
		height : ($(window).height() - 200) + 'px',
		position : 'top'
	}); 
	selected = {
		env : $(".radioURL select option:first").text(),
		tree : '',
		tab : $('#jqxTabs').jqxTabs('selectedItem')
	};
	getjqxTree($(".radioURL select option:first").text());
	$(".searchSource").val(""); 
	$(".testClwss").tooltip();	
	$('.jqx-tabs-headerWrapper').hide();
	$('#alertsTable').hide();
	setInitialdisplay();
	
});

function setJqTree(dataforjqxtree) { 
setsourceforjqxtree = [ {
		label : "Datasource",
		selected : true,
		expanded : true,
		items : dataforjqxtree
	}];
		
	$('#jqxTree').jqxTree({
		source : setsourceforjqxtree,
		width : '100%'
	});
	updateSelected("tree", setsourceforjqxtree[0].label);


}

$(".radioURL select").change(function(event) {
	firstload = [];
	jqxtreedata = [];
	initialloaddata = [];
	updateSelected('env', $(this).val());
	getjqxTree($(this).val());
	clearTable();
	getDatas(event);
});

$('#jqxTree').on('select', function(event) {
	var args = event.args;
	var item = $('#jqxTree').jqxTree('getItem', args.element);
	setbreadcrumbs(item);
	clearTable();
	setdisplay(item.label);
});

$('#jqxTabs').on('selected', function(event) {
	updateSelected('tab', $('#jqxTabs').jqxTabs('selectedItem'));
	getData(); 
});

function setdisplay(selectedjqxtreelabel) {	

	updateSelected('tree', selectedjqxtreelabel);
	item = $('#jqxTree').jqxTree('getSelectedItem');
	if (!($('#jqxTree').jqxTree('getItem', item).parentId)) {
		$('#jqxTabs').jqxTabs({
			selectedItem : 0
		}); 
		updateSelected('tab', $('#jqxTabs').jqxTabs('selectedItem'));  
		$('#alertsTable').hide();
		setInitialdisplay();
	} else {
		$('#alertsTable').show();
		getData();
	}
}

function updateSelected(term, value) {
	selected[term] = value; 
}

function clearTable() {
	$("#detailsTable tr:gt(1)").remove();
	$("#alertsTable tr:gt(1)").remove();
	$("#statusTable tr:gt(0)").remove(); 
	dontreloadLoadHistoryTab = false; 
}

$(".searchSource").keyup(function() {
	if ($(this).val() == "") {
		resetDetailsTable();
		$("#statusTable tr").show(); // When the searchSource is empty, the status all rows in status table should be seen.
	}
});

function resetDetailsTable() {
	$(".searchSource").val("");
	$("#detailsTable tr:gt(1)").each(function() {
		if ($(this).hasClass("newRow")) {
			$(this).show();
			$(this).find("button").text("Show");
		} else {
			$(this).remove();
		}
	});
}

function setbreadcrumbs(item) {
	var root = $('#jqxTree').jqxTree('getItem', item.parentElement);

	if (root == null) {
		$('.breadcrumb').html("<li class='active'>" + item.label + "</li>");
	} else if ($('#jqxTree').jqxTree('getItem', root.parentElement) == null) {
		$('.breadcrumb').html(
				"<li class='linkbackbreadcrumbs'>" + root.label
						+ "</li><li class='active'>" + item.label + "</li>");
	}
}

$(document).on(
		'click',
		'.partitionshow',
		function() {
			
			if ($(this).text() == "Show") {
				resetDetailsTable();
				$("#detailsTable tr:gt(1)").not($(this).closest("tr")).hide();
				displayPartitionColumns($(this).closest("tr").children(
						"td:eq(0)").text());
				$(this).text("Hide");
			} else {
				resetDetailsTable();
			}
		});

$(document).on('click', '.columnsshow', function() {
	if ($(this).text() == "Show") {
		resetDetailsTable();
		$("#detailsTable tr:gt(1)").not($(this).closest("tr")).hide();
		displayColumns($(this).closest("tr").children("td:eq(0)").text());
		$(this).text("Hide");
	} else {
		resetDetailsTable();
	}
});

$(document).on('click', '.linkbackbreadcrumbs', function() {
	var items = $('#jqxTree').jqxTree('getItems');
	for (i = 0; i < items.length; i++) {
		if (items[i].label == $(this).text()) {
			$('#jqxTree').jqxTree('selectItem', items[i]);
			setbreadcrumbs(items[i]);
			break;
		}
	}
});






