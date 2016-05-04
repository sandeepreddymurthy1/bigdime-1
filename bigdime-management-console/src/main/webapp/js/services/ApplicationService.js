/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * @author Sandeep Reddy,Murthy
 * @ngdoc function
 * @name jsonerApp.ApplicationService
 * @description # Provides application specific services
 */
angular.module('jsonerApp').factory('ApplicationService',
		function($http, $rootScope) {
	        var envConstants=[];
			var applicationService={};
			applicationService.getjqxTree = getjqxTree;
			applicationService.getDates=getDates;
			applicationService.getAlertData=getAlertData;
			applicationService.setEnvConstants=setEnvConstants;
			applicationService.getEnvConstants=getEnvConstants;
			applicationService.envConstants=[];
			return applicationService;
            
			function setEnvConstants(){
				var envDetails={};
				$.ajax({
					  dataType: "json",
					  url:'rest/propertyService/applicationproperties',
					  async: false,
					  success: function(data) { 
						 envDetails=data; 
						}
				});
				applicationService.envConstants=[{
			 	    "environment":"Dev",
			 	    "url": envDetails.devHost,
			        "application":envDetails.application,
			         "port": envDetails.devPort
			         },{
			     	    "environment":"Qa",
			            "url": envDetails.qaHost,
			            "application":envDetails.application,
			            "port": envDetails.qaPort
			             },{
			         	    "environment":"Prod",
			                "url": envDetails.prodHost,
			                "application":envDetails.application,
			                "port": envDetails.prodPort
			}];		
			}
			function getEnvConstants(){
				if(applicationService.envConstants ==undefined || applicationService.envConstants.length==0){
					applicationService.setEnvConstants();					
				}
				return applicationService.envConstants;
			}
			function getjqxTree(environment,callback) {				
				$.each(applicationService.getEnvConstants(),function(index,value){
					if(applicationService.getEnvConstants()[index].environment.toLowerCase()===environment.toLowerCase()){
						$http.get(applicationService.getEnvConstants()[index].url+applicationService.getEnvConstants()[index].port+"/"+applicationService.getEnvConstants()[index].application+'/rest/alertService/alertset').success(function(response){
					   	callback(response);
						});
					}
				});
			}
			
			function getDates(){
				$http.post('').succes(function(responsse){
					callback(response);
				});
			}
			
			function setApplicatioNamesinApplicationList(name){
				$rootScope.applicationlist=[];
				var application=applicationFactory.getApplicationObject();
				application.name=name;
				$rootScope.applicationlist.push(application);
			}
			
			function setApplicationDateForApplication(name,date){
			    $.each(applicationlist,function(index,value){

			    });
			}
			
            function getAlertData(applicationname,offsetdate,callback){
            	$.each(applicationService.getEnvConstants(),function(index,value){
            		if($rootScope.environment.selected !==null ||$rootScope.environment.selected !==undefined ||$rootScope.environment.selected !==""){
            		  if(applicationService.getEnvConstants()[index].environment.toLowerCase()===$rootScope.environment.selected.toLowerCase()){
            			  console.log(applicationService.getEnvConstants()[index].url+applicationService.getEnvConstants()[index].port+"/"+applicationService.getEnvConstants()[index].application+'/rest/alertService/recentalerts?alertName='+applicationname+'&start='+offsetdate+'&limit=20');
            			  $http.get(applicationService.getEnvConstants()[index].url+applicationService.getEnvConstants()[index].port+"/"+applicationService.getEnvConstants()[index].application+'/rest/alertService/recentalerts?alertName='+applicationname+'&start='+offsetdate+'&limit=20').success(function(response){
      					   	callback(response);
      						});
            	     	}
            		}
            	});
            }
            
            function getDates(applicationname,offsetdate,callback){
            	$.each(applicationService.getEnvConstants(),function(index,value){
            		if($rootScope.environment.selected !==null ||$rootScope.environment.selected !==undefined ||$rootScope.environment.selected !==""){
            		  if(applicationService.getEnvConstants()[index].environment.toLowerCase()===$rootScope.environment.selected.toLowerCase()){
            			  $http.get(applicationService.getEnvConstants()[index].url+applicationService.getEnvConstants()[index].port+"/"+applicationService.getEnvConstants()[index].application+'/rest/alertService/dates?alertName='+applicationname+'&start='+offsetdate).success(function(response){
      					   	callback(response);
      						});
            	     	}
            		}
            	});
            }
		});
