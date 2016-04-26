angular.module('jsonerApp')
.filter('formatDate',function(){
	return function(inputLongFormatdate){
		var dateArray=new Date(inputLongFormatdate).toGMTString().split(" ");
		return dateArray[0]+" "+dateArray[2]+" "+dateArray[1]+" "+dateArray[4]+" UTC "+dateArray[3];
	};
});