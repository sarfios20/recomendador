$( document ).ready(function() {
		$.get('option_users.php',
			function(data, status) {
				if (status === 'success') {
					$('#usuarios').html(data);
				}
			})
        $.get('option_films.php',
            function(data, status) {
                if (status === 'success') {
                    $('#peliculas').html(data);
                }
            })	
});	



function genera_tabla(json) {
	console.log(json);
	html='<tr>'
	for (const val of json) {
		html=html+'<th>'+val[0]+'</th><th>'+val[1]+'</th><th>'+val[2]+'</th>'
		html=html+'</tr>'
   		console.log(val);
   	}

   	$('#tabla').append(html);
}

function predecir(obj){
	loader(obj);
	UserID = $("#numero_usuario").text();
	console.log(UserID);
	MovieID = $("#peliculas").val();
	Umbral = $("#umbral2").val();

	document.getElementById("destino").innerHTML = "";

    $.ajax({
		url: "predecir.php",
		type: "post",
		data: {id: UserID, movieID:MovieID, umbral:Umbral} ,
        success: function (response) {
			json = JSON.parse(response)
			for (pelicula in json){
				document.getElementById("destino").innerHTML = "Prediccion"+String(json[pelicula]);
				spin = $('.loader');
				spin.remove();
				obj.style.display = '';
			}
        },
        error: function(jqXHR, textStatus, errorThrown) {
           console.log(textStatus, errorThrown);
		   	spin = $('.loader');
			spin.remove();
			obj.style.display = '';
        }
    });
}

function update(){
	UserID = $("#usuarios").val();
	div = $("#numero_usuario").html(UserID);
}

function loader(obj){
	obj.style.display = 'none';
	o = $(obj)
	spin = $('<div class="loader"></div>');	
	afterNode = o.next();
	spin.insertBefore(afterNode);
}


function ajax(obj){
	loader(obj);
	UserID = $("#usuarios").val();
	Umbral = $("#umbral").val();
	Predicciones = $("#predicciones").val();
	
	var tabla = document.getElementById("tabla");
	
	if (tabla.hasChildNodes()) {	
		while (tabla.firstChild) {
			tabla.removeChild(tabla.firstChild);
		}
	}

    $.ajax({
		url: "user-user.php",
		type: "post",
		data: {id: UserID, umbral:Umbral, predicciones: Predicciones} ,
        success: function (response) {
			spin = $('.loader');
			spin.remove();
			obj.style.display = '';
			json = JSON.parse(response)
			genera_tabla(json)
        },
        error: function(jqXHR, textStatus, errorThrown) {
			console.log(textStatus, errorThrown);
		   	spin = $('.loader');
			spin.remove();
			obj.style.display = '';
        }


    });
	
	
}
