%% ----------------------------------------------------------------------------
%% paxos : Modulo la estructura de datos principal de Paxos
%%
%% 
%% 
%% 
%% 
%% ----------------------------------------------------------------------------


-module(datos_paxos).
-compile(export_all).
-export([setFiabilidad/2, setNumMensajes/2, getFiabilidad/1,
	getNumMensajes/1, addInstanciaProponente/2, getInstanciasProponentes/1, existeInstanciaProponentes/2, existeRegistro/2,
	addInstanciaAceptador/2, getInstanciasAceptadores/1, existeInstanciaAceptadores/2, getRegistroValor/2, mensajeAdd/1,
	getInstanciaMaximaVista/1 ,getInstanciaMinima/1, setInstanciaMinima/2, intentarSetInstanciaMaximaVista/2 ,setRegistro/2, borrarEntradaRegistro/2]).
-record(paxos, {fiabilidad = fiable,
                num_mensajes = 0,
		instanciaMaxima = 0,
		instanciaMinima = 0,
		instanciasProponentes = dict:new(),
		instanciasAceptadores = dict:new(),
		registro = dict:new()}).
                




%%%%%%%%%%% FUNCIONES DE ACCESO Y MANIPULACION DE LA ESTRUCTURA DE DATOS GETS AND SETS

crearRegistro() ->
	#paxos{}.

addInstanciaProponente(Registro, {Numero, Valor}) ->
	Registro#paxos{instanciasProponentes=dict:append(Numero, Valor, getInstanciasProponentes(Registro))}.

addInstanciaAceptador(Registro, {Numero, Valor}) ->
	Registro#paxos{instanciasAceptadores=dict:append(Numero, Valor, getInstanciasAceptadores(Registro))}.

addRegistro(Registro, {Numero, Valor}) ->
	Registro#paxos{registro=dict:append(Numero, Valor, getRegistro(Registro))}.

setFiabilidad(Registro, Fiab) ->
	Registro#paxos{fiabilidad=Fiab}.

setNumMensajes(Registro, Num) ->
	Registro#paxos{num_mensajes=Num}.

mensajeAdd(Registro) ->
	Registro#paxos{num_mensajes=getNumMensajes(Registro)+1}.

getFiabilidad(Registro) ->
	Registro#paxos.fiabilidad.

getNumMensajes(Registro) ->
	Registro#paxos.num_mensajes.


getInstanciaMaximaVista(Registro) ->
	Registro#paxos.instanciaMaxima.

getInstanciaMinima(Registro) ->
	Registro#paxos.instanciaMinima.

%solo hace le set si el valor es mayor del que está en el registro
intentarSetInstanciaMaximaVista(Registro, Valor) ->
	ValorActual=getInstanciaMaximaVista(Registro),
	if ValorActual < Valor -> Registro#paxos{instanciaMaxima=Valor};
	true -> Registro#paxos{instanciaMaxima=ValorActual}
	end.


setInstanciaMinima(Registro, Valor) ->
	Registro#paxos{instanciaMinima=Valor}.

setRegistro(Registro, Nuevo) ->
	Registro#paxos{registro=Nuevo}.

getInstanciasProponentes(Registro) ->
	Registro#paxos.instanciasProponentes.

getInstanciasAceptadores(Registro) ->
	Registro#paxos.instanciasAceptadores.

borrarEntradaRegistro(N, Registro)->
	setRegistro(Registro, dict:erase(N, getRegistro(Registro))). 

getRegistro(Registro) ->
	Registro#paxos.registro.

existeInstanciaProponentes(Registro, NuInstancia) ->
	dict:is_key(NuInstancia, getInstanciasProponentes(Registro)).

existeInstanciaAceptadores(Registro, NuInstancia) ->
	dict:is_key(NuInstancia, getInstanciasAceptadores(Registro)).

getRegistroValor(Registro, NuInstancia) ->
	dict:fetch(NuInstancia, getRegistro(Registro)).

existeRegistro(Registro, NuInstancia) ->
	dict:is_key(NuInstancia, getRegistro(Registro)).

