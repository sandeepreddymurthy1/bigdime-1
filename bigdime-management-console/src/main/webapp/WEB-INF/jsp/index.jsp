<%--

    Copyright (C) 2015 Stubhub.

--%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Bigdime management console</title>
<link href="${pageContext.request.contextPath}/resources/css/hot-sneaks/jquery-ui-1.10.4.custom.css" rel="stylesheet"/>
<link type="text/css" href="${pageContext.request.contextPath}/resources/css/hot-sneaks/style.css" rel="stylesheet">
<link rel="stylesheet" href="${pageContext.request.contextPath}/resources/jquery/jqwid/Styles/jqx.base.css" type="text/css" />
<link rel="stylesheet" href="${pageContext.request.contextPath}/resources/bootstrap/css/bootstrap.min.css" type="text/css" />

</head>
<body >
 <div>
	    <div class="row1">
	       <h class="page-header">Bigdime Management Console</h>
	       <p class="home"> <label>Welcome </label></p>
	</div>
	<form action ="home" method="POST"id="formID">
    <div class="container">
	    <div class="row12">
			<div class="loginbody ">
			  <label class="glyphicon glyphicon-user"id="u1">
			     Username</label></br>      <input type="text"class="text" required name="userName"  autofocus id="txtUserName" placeholder="username"/><br/><br/>
			    <label class= "glyphicon glyphicon-lock" id="u1">
			     Password </label></br>    <input type="password" class="text" id="txtPassword" name="password" required="required"placeholder="password"/><br/><br/>
			    <button type="submit" value="Login" onSubmit="validateForm();">Login</button>
		    </div>
		      <%if (session.getAttribute("loginResult")=="error") {%>
			   <p class="centre-align-invalid-credentials">Invalid UserName or Password.Please check </p>
			   <%} else if(session.getAttribute("loginResult")=="success"){session.invalidate();} %>
			   			   
	    </div>
    </div>
    <footer><nav class="navbar  navbar-default navbar-fixed-bottom"><div class="f1">Powered by BigDime@copyrights-2015</div></nav></footer>
</form>
 
</div>
</body>
</html>