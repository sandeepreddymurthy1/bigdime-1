/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * @author Sandeep Reddy,Murthy
 * @ngdoc function
 * @name jsonerApp.SharedService
 * @description # Provides Shared services which will be used by multiple controllers
 */

angular.module('jsonerApp').factory('SharedService',function($rootScope,$timeout){
	var sharedService={};
	  sharedService.applicationselected="";
      sharedService.adaptorList=[];
	  sharedService.prepareBroadCast=function(applicationname){
		  this.applicationselected=applicationname;
		  sharedService.broadcastchange();  
	  };
	  sharedService.broadcastchange=function(){
//		  $timeout(function(){
		  $rootScope.$broadcast('handleApplicationchangedBroadcast');
//	       },1000);
	  };
	  
	  sharedService.highlightbreadcrumb=function(){
			var $thumbs = $('li .breadcrumbtext').click(function(e) {
			    e.preventDefault();
			    $thumbs.removeClass('highlight');
			    $(this).addClass('highlight');
			});
	  };
	  
	  sharedService.updatebreadcrumbonadadaptorchange=function(){		  
		  $rootScope.breadcrumbs={
		    		 "adaptor":"true",
		    		 "source":"false",
		    		 "sink":"false",
		    		 "channel":"false",
		    		 "mapping":"false",
		    		 "result":"false"
		     };		  
      };
	  
      sharedService.getadaptorlist=function(){
   	   $.each(AdaptorConstants,function(index,value){
			 if($rootScope.environment.selected.toLowerCase()===AdaptorConstants[index].environment.toLowerCase()){
				 adaptorList= AdaptorConstants[index].adaptors;
			 }
		 });
      };
      
      sharedService.gethandlertList=function(){
   	   $.each(AdaptorConstants,function(index,value){
   		 if($rootScope.environment.selected.toLowerCase()===AdaptorConstants[index].environment.toLowerCase()){
   			 $.each(AdaptorConstants[index].adaptors,function(adaptorindex,value){	
   				 if($rootScope.adaptor.selected.toLowerCase()===AdaptorConstants[index].adaptors[adaptorindex].name.toLowerCase()){
   			          $scope.handlerlist= AdaptorConstants[index].adaptors[adaptorindex].handlerlist;	
   				 }
   			 });
   		   }
   	        });
      };
	
	  
	   return sharedService; 
});