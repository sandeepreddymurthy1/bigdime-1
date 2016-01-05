/*
 * Copyright (C) 2015 Stubhub.
 */
$(document).ready(function(){
 
  $("#searchAlertSourceID").keyup(function(){
    setAlertTable([$("#searchAlertHostID").val(),$("#searchAlertSourceID").val(),$("#searchAlertStartTime").val(),$("#searchAlertEndTime").val(),$("#searchAlertEventID").val()]);

  });

  $("#searchAlertHostID").keyup(function(){
    setAlertTable([$("#searchAlertHostID").val(),$("#searchAlertSourceID").val(),$("#searchAlertStartTime").val(),$("#searchAlertEndTime").val(),$("#searchAlertEventID").val()]);

  });

  $("#searchAlertEventID").keyup(function(){
    setAlertTable([$("#searchAlertHostID").val(),$("#searchAlertSourceID").val(),$("#searchAlertStartTime").val(),$("#searchAlertEndTime").val(),$("#searchAlertEventID").val()]);
  });
   $('#datetimepicker1').datetimepicker(); 
   $('#datetimepicker2').datetimepicker();
             
});


function resetAlertTable(){
$("#searchAlertSourceID").val("");
  $("#searchAlertHostID").val("");
  $("#searchAlertTimeID").val("");
  $("#searchAlertEventID").val("");

}

function setAlertTable(searchAlertArray){
  searchAlertArrayLength= searchAlertArray.length;
  var searchAlertLogic=new Array($("#alertsTable tr:gt(1)").length);
  for(i = 0; i< searchAlertLogic.length; i++){
    searchAlertLogic[i] = new Array(searchAlertArrayLength);
  }
  for(var i = 0; i<searchAlertArrayLength;i++ ){
    var j = 0;  
    $("#alertsTable tr:gt(1)").each(function(){
        if(i == 2){
            if(searchAlertArray[i]==null || searchAlertArray[i] == undefined || searchAlertArray[i]==''){
            searchAlertLogic[j][i]=true;
            }
            else{
                    var newselecteddate =moment(new Date(searchAlertArray[i]));
                    momentNewSelectedDate = moment(newselecteddate).format("x");
                    var actualdate = $(this).find("td:eq("+i+")").text();
                     momentActualDate = moment(actualdate).format("x");
                   if((momentActualDate - momentNewSelectedDate) >= 0){
                   searchAlertLogic[j][i]=true;
                   }
                    else {
                        searchAlertLogic[j][i]=false;
                    }
            }
        }
        else if(i == 3){
            
            if(searchAlertArray[i]==null || searchAlertArray[i] == undefined || searchAlertArray[i]==''){
            searchAlertLogic[j][i]=true;
            }
            else{
                    var newselecteddate =moment(new Date(searchAlertArray[i]));
                    momentNewSelectedDate = moment(newselecteddate).format("x");
                    var actualdate = $(this).find("td:eq("+i+")").text();
                     momentActualDate = moment(actualdate).format("x");
                   if((momentActualDate - momentNewSelectedDate) <= 0){
                   searchAlertLogic[j][i]=true;
                   }
                    else {
                        searchAlertLogic[j][i]=false;
                    }
            }
            
        }
        else{
       var searchAlertText = $(this).find("td:eq("+i+")").text().toUpperCase();
        if(searchAlertText.indexOf(searchAlertArray[i].toUpperCase())!=-1 || searchAlertText == ''){
          searchAlertLogic[j][i]=true;
        }
        else{
          searchAlertLogic[j][i]=false;
        }
        }
        j++;
    })
  }
  
  for(var i = 0; i < searchAlertLogic.length; i ++){
    console.log(searchAlertLogic[i]);
    var output = true;
      for(var j = 0; j < searchAlertArrayLength; j++){
        output = searchAlertLogic[i][j] && output; 
        
      }
     
      if(output){

        $("#alertsTable tr:eq("+parseInt(i+2)+")").show();
      }
      else{
        $("#alertsTable tr:eq("+parseInt(i+2)+")").hide();
      }
  }

}


$("#datetimepicker1").on("dp.change", function(e) {
	   var selectedStartDate = $(this).find("input").val();
	 setAlertTable([$("#searchAlertHostID").val(),$("#searchAlertSourceID").val(),selectedStartDate,$("#searchAlertEndTime").val(),$("#searchAlertEventID").val()]);

	});
	$("#datetimepicker2").on("dp.change", function(e) {
	  var selectedEndDate = $(this).find("input").val();
	    setAlertTable([$("#searchAlertHostID").val(),$("#searchAlertSourceID").val(),selectedEndDate,$("#searchAlertEndTime").val(),$("#searchAlertEventID").val()]);

	});