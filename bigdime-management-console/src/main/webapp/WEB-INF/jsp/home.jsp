<%--

    Copyright (C) 2015 Stubhub.

--%>

<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Bigdime Management Console</title>
<link
	href="${pageContext.request.contextPath}/resources/css/hot-sneaks/jquery-ui-1.10.4.custom.css" rel="stylesheet">
<link
	href="${pageContext.request.contextPath}/resources/css/hot-sneaks/jquery-ui.css" rel="stylesheet">
<link type="text/css"
	href="${pageContext.request.contextPath}/resources/css/hot-sneaks/style.css" rel="stylesheet">
<link rel="stylesheet"
	href="${pageContext.request.contextPath}/resources/jquery/jqwid/Styles/jqx.base.css" type="text/css">
<link rel="stylesheet"
	href="${pageContext.request.contextPath}/resources/bootstrap/css/bootstrap.min.css" type="text/css">
<link rel="stylesheet"
	href="${pageContext.request.contextPath}/resources/bootstrap/css/bootstrap-datetimepicker.min.css" type="text/css">
</head>
<body>
	<div>
		<div class="row1">
			<h class="page-header">Big Data Management Console</h>
			<p class="home">
				Welcome ${userID}, you are logged in</br> <a href="${pageContext.request.contextPath}/">Log out</a>
			</p>
			<p>
			<ul class="nav navbar-nav navbar-right">
				<li class="radioURL"><select style="width:auto" class="form-control" id="e1">
					    <option>Dev</option>
						<option>Qa</option>	
						<option>Prod</option>					
				</select></li>
			</ul>
			</p>
		</div>
		<div class="row2">
			<ol class="breadcrumb">
			</ol>
		</div>

		<div class="row3">
			<div class="col-md-2 col-lg-2 noleftpadding norightpadding">
				<div id='jqxTree'></div>
			</div>
			<div id="thirdBox" class="col-md-8 col-lg-8 norightpadding" >	
             <div id='jqxTabs'>			
					<ul id='jqxTabsUl'>				    
						<li style="display:none">Alerts</li>									
						</ul>
					<div class="fullborder">
						<table id="alertsTable"
							class="table table-bordered table-striped">
							<tr>
								<td colspan="7">
									<div class="vssearch" style="width:100%;">
									<div class="col-md-2 col-lg-2 nopadding" style="width:100%">
											<input type="text" class="noborder form-control searchAlertSource leftborderradius rightborderradius"
												placeholder="Search" id="searchAlertSourceID">
										</div>
<!-- 										<div class="col-md-3 col-lg-3 nopadding" style="visibility: collapse;">
											<div class='input-group date' id='datetimepicker2'>
												<input type='text' class="form-control noborder"
													placeholder="End Date" id="datetimepicker" /> <span
													class="input-group-addon"> <span
													class="glyphicon glyphicon-calendar"></span>
												</span>
											</div>
										</div>
										<div class="col-md-2 col-lg-2 nopadding" style="visibility: collapse;">
											<input type="text" class=" noborder form-control searchAlertEvent rightborderradius"
												placeholder="search event type" id="searchAlertEventID">
										</div> -->
									</div>
								</td>
							</tr>
						</table>
					</div>
			</div>
		</div>
		<footer>
			<nav class="navbar  navbar-default navbar-fixed-bottom">
				<div class="f2">Powered by BigDime@copyrights-2015</div>
			</nav>
		</footer>
	</div>
	<script>
		
	<%response.setHeader("Cache-Control",
					"no-cache, no-store, must-revalidate"); // HTTP 1.1.
			response.setHeader("Pragma", "no-cache");
			response.setHeader("Expires", "0");// HTTP 1.0.
			if (session.getAttribute("valid") == null) {
				out.println("<script>parent.location.href='index.jsp'</script>");
			}%>
		
	</script>
	<script src="${pageContext.request.contextPath}/resources/jquery/moment.min.js"></script>
	<script src="${pageContext.request.contextPath}/resources/jquery/jquery-1.10.2.js"></script>

	<script src="${pageContext.request.contextPath}/resources/jquery/jquery-ui-1.10.4.custom.js"></script>
	<script src="${pageContext.request.contextPath}/resources/jquery/scripts/demos.js"></script>
	<script src="${pageContext.request.contextPath}/resources/jquery/jqwid/jqxcore.js"></script>
	<script src="${pageContext.request.contextPath}/resources/jquery/jqwid/jqxtree.js"></script>
	<script src="${pageContext.request.contextPath}/resources/jquery/jqwid/jqxexpander.js"></script>
	<script src="${pageContext.request.contextPath}/resources/jquery/jqwid/jqxtabs.js"></script>


	<script src="${pageContext.request.contextPath}/resources/bootstrap/js/bootstrap.min.js"></script>
	<script src="${pageContext.request.contextPath}/resources/bootstrap/js/bootstrap-datetimepicker.min.js"></script>
	<script src="${pageContext.request.contextPath}/resources/jquery/scripts/underscore-1.6.0.js"></script>
	<script src="${pageContext.request.contextPath}/resources/jquery/scripts/backbone-1.1.2.js"></script>

	<script src="${pageContext.request.contextPath}/resources/jquery/Controller/OverviewController.js"></script>
	<script src="${pageContext.request.contextPath}/resources/jquery/Controller/AlertController.js"></script>
	<%--  <script src="${pageContext.request.contextPath}/resources/jquery/Controller/configController.js" type="text/javascript"></script> --%>


	 <script src="${pageContext.request.contextPath}/resources/jquery/Model/bbRoutes.js" type="text/javascript"></script>
	 <script
		src="${pageContext.request.contextPath}/resources/jquery/Model/bbModel.js"
		type="text/javascript"></script>
	<%-- <script src="${pageContext.request.contextPath}/resources/jquery/Model/configModel.js" type="text/javascript"></script> --%>


	<script src="${pageContext.request.contextPath}/resources/jquery/Views/OverView.js"></script>
 <script src="${pageContext.request.contextPath}/resources/jquery/Views/alertView.js"></script> 
	<%--  <script src="${pageContext.request.contextPath}/resources/jquery/Views/configview.js" type="text/javascript"></script> --%>

</body>
</html>