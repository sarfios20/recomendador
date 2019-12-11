<?php
	require('db.php');
	$sel_query="SELECT distinct movieId, title FROM `movies`;";
	$result = mysqli_query($con,$sel_query);
	
	$filas = "";
	while($row = mysqli_fetch_assoc($result)) {
		
		$filas .= "<option value='".$row["movieId"]."'>'".$row["movieId"]." ".$row["title"]."'</option>";


	}

	echo utf8_encode($filas);
	
?>