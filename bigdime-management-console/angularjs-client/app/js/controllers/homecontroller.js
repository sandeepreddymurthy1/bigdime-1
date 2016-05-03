/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * @author Sandeep Reddy,Murthy
 * @ngdoc function
 * @name jsonerApp.controller:HomecontrollerCtrl
 * @description #HomecontrollerCtrl Home Controller of the jsonerApp
 */

'use strict';

angular.module('jsonerApp').controller(
		'HomecontrollerCtrl',
		function($scope, $location, $rootScope, ApplicationService, $filter,
				SharedService,AdaptorSharedService,jsonBuilderFactory) {
			$scope.environmentoptions = [ 'Dev', 'Qa', 'Prod' ];
			$scope.environment = {};
			$scope.environment.selected = $scope.environmentoptions[0];
			$scope.userid = $rootScope.globals.currentUser.username;
			$scope.displayName = $rootScope.globals.currentUser.displayName;
			$scope.init = function() {
				$scope.getjqxTree();
			};
			$scope.getjqxTree = function() {
				$rootScope.environment = $scope.environment;
				ApplicationService.getjqxTree($scope.environment.selected,
						function(response) {
							$scope.applicationlist = response;
							$rootScope.applicationlist = response;
							SharedService.prepareBroadCast(response[0].label);
						});
			};
			$scope.init();
			$scope.getDates = function(applicationname) {
				$rootScope.selectedapplication = applicationname;
				ApplicationService.getDates(applicationame, function(response) {
					$.each($scope.applicationlist, function(index, value) {
						if (value['name'].tolowercase() === applicationame
								.tolowercase()) {
							$scope.applicationlist[index].datesArray
									.push(response);
						}
					});
				});
			};

			$scope.updateRoot = function(applicationname) {
				SharedService.prepareBroadCast(applicationname);
			};
			$scope.logout =function(){
				AdaptorSharedService.resetbreadcrumbs();
				jsonBuilderFactory.resetBuiltAdaptor();
			};

			var $thumbs = $('li .breadcrumbtext').click(function(e) {
				e.preventDefault();
				$thumbs.removeClass('highlight');
				$(this).addClass('highlight');
			});

		});