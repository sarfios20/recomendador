<?php
	$id = $_POST['id'];
	$umbral = $_POST['umbral'];
	$predicciones = $_POST['predicciones'];
	$directorio = __DIR__;
	$output = shell_exec("python ".$directorio."\user-user.py ".$id." ".$umbral." ".$predicciones);
	echo $output;

?>