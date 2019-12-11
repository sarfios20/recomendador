<?php
	require('db.php');
	$sel_query="SELECT distinct userId FROM `ratings`;";
	$result = mysqli_query($con,$sel_query);
	
	$filas = "";
	while($row = mysqli_fetch_assoc($result)) {
		
		$filas .= "<option value='".$row["userId"]."'>'".$row["userId"]."'</option>";


	}

	echo utf8_encode($filas);
	
?>