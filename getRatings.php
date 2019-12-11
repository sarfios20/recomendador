<?php
	require('db.php');

	$sel_query="SELECT userId, movieId, rating FROM `ratings`;";
	$result = mysqli_query($con,$sel_query);

	
	$data = "{ "; 
	$user = "";
	$first = true;
	while($row = mysqli_fetch_assoc($result)) { 
		

		if($user == $row["userId"]){

			
			


			$data = $data . ',' .$row["movieId"].  ':'  .$row["rating"]  ;

		}else{
			if($first){
				$first= false;
				$user = $row["userId"];
				$data = $data . $user. ':' . '{';
				$data = $data . $row["movieId"].  ':'  .$row["rating"]  ;

			}else{

				$user = $row["userId"];
				$data = $data . '},' .$user. ':' . '{';
				$data = $data . $row["movieId"].  ':'  .$row["rating"]  ;



			}
		}

		
	}
	$data = substr($data, 0, -1)."}"; // se quita la ?ltima coma y se cierra el array
	
	// se devuelve el resultado
	echo utf8_encode($data);
	
?>