<?php
	require('db.php');
	
	$id = $_POST['id'];
	$sel_query="SELECT `title` FROM `movies` WHERE `movieId`=".$id.";";
	$result = mysqli_query($con,$sel_query);
	$fila = mysqli_fetch_row($result);
	echo $fila[0];
?>