<?php

	
	$id = $_POST['id'];
	$rawTitle = $_POST['movieID'];
	$umbral = $_POST['umbral'];
	
	$title = "\"".$rawTitle."\"";
	
	$directorio = __DIR__;
	$output = shell_exec("python ".$directorio."\predecir.py ".$id." ".$umbral." '".$title."'");
	
	echo $output;

?>