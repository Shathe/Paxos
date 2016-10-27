
%% ----------------------------------------------------------------------------
%% paxos : Modulo proponente paxos
%%
%% 
%% 
%% 
%% 
%% ----------------------------------------------------------------------------

-module(aceptador).


-export([aceptadorUnaInstancia/5, aceptador/2]).
-export([comprobar_estado_paxos/1]).

-define(T_ESPERAA, 150).
-define(T_ESPERAB, 60).
-define(T_ETAPA, 2000).
-define(PRINT(Texto,Datos), io:format(Texto, Datos)).
-define(ENVIO(Mensj, Dest),
        io:format("Llega a nodo ~p se envia ~p a ~p~n",[node(), Mensj, Dest]), Dest ! Mensj).
-define(ENVION(Mensj, Dest), Dest ! Mensj).
-define(ESPERO(Dato), Dato -> io:format("LLega ~p-> ~p~n",[Dato,node()]), ).


%%%%%%%%%%%% FUNCIONES EXPORTABLES  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%Proceso que gestiona todos los aceptadores del nodo para cada instancia
aceptador(Servidores, ListaAceptadores)->

	receive

		{prepara, N, NodoFrom, NumInstancia} -> 
			Existe = dict:is_key(NumInstancia, ListaAceptadores),
			if Existe -> % Si existía el proceso, reenviarle el mensaje
				{ok,  [PidAceptador|_]} = dict:find(NumInstancia, ListaAceptadores),
				?ENVION({prepara, N, NodoFrom, NumInstancia}, PidAceptador),
				aceptador(Servidores,ListaAceptadores);

			true -> %Sino existía el proceso, crearlo, guardarlo, reenviarlo
				Proc=spawn_link(?MODULE, aceptadorUnaInstancia, [Servidores, NumInstancia, -1, -1, -1]),
				paxos ! {addInstanciaAceptadores, NumInstancia, Proc},
				NuevaListaAceptadores = dict:append(NumInstancia, Proc, ListaAceptadores),
				Proc ! {prepara, N, NodoFrom, NumInstancia},
				spawn_link(?MODULE, comprobar_estado_paxos, [Proc]),
				aceptador(Servidores, NuevaListaAceptadores)
			end;




		{acepta, {NumInstancia, Valor}, N, NodoFrom} -> 
			Existe = dict:is_key(NumInstancia, ListaAceptadores),
			
			if Existe -> % Si existía el proceso, reenviarle el mensaje
				{ok, [PidAceptador|_]} = dict:find(NumInstancia, ListaAceptadores),
				PidAceptador ! {acepta, {NumInstancia, Valor}, N, NodoFrom},
				aceptador(Servidores, ListaAceptadores);

			true -> %c%Sino existía el proceso, crearlo, guardarlo, reenviarlo
				Proc=spawn_link(?MODULE, aceptadorUnaInstancia, [ Servidores, NumInstancia, -1, -1, -1]),
				paxos ! {addInstanciaAceptadores, NumInstancia, Proc},
				NuevaListaAceptadores = dict:append(NumInstancia, Proc, ListaAceptadores),
				Proc ! {acepta, {NumInstancia, Valor}, N, NodoFrom},
				Proc ! crear_control,
				aceptador(Servidores, NuevaListaAceptadores)
			end;


		Mensaje   -> io:format("Mensaje en proponente no identificado ~p, ~p ~n",[node(), Mensaje]),
			     aceptador(Servidores, ListaAceptadores)
	end.

 
%%Proceso que gestiona el aceptador de una unica instnacia
aceptadorUnaInstancia(Servidores, NumeroInstancia, N_p, N_a, V_a )->
%n_p : mayor valor visto en mensajes prepara
%n_a, v_a : mayores valores vistos en mensajes acepta
%V valor de la instancia

	receive

		{prepara, N, NodoFrom, NumInstancia} -> 
			%Comprobar si por la instancia que te preguntan ya hay valor decidido
			%Y responderle con el valor decidido de la instancia
			{ExisteEstaInstancia, ValorAhora} = paxos:estado(node(), NumInstancia),
			if ExisteEstaInstancia ->
				io:format("Va atrasado  ~p , ~p~n",[NodoFrom, {NumInstancia,ValorAhora}]),
				{paxos, NodoFrom} ! {decidido, {NumInstancia, ValorAhora}};
			true->%Gestionar mensaje prepara

			    if
				N > N_p -> %Enviar acepta
				    {paxos, NodoFrom} ! {prepara_ok, N, N_a, V_a, node(), NumInstancia},
				    aceptadorUnaInstancia(Servidores, NumeroInstancia, N, N_a, V_a);
				true -> % Enviar el reject
				    {paxos, NodoFrom} ! {prepara_reject, N_p, node(), NumInstancia},
				    aceptadorUnaInstancia(Servidores, NumeroInstancia,  N_p, N_a, V_a)
			    end
			end;



		{acepta, {NumInstancia, Valor}, N, NodoFrom} -> 
			%Comprobar si por la instancia que te preguntan ya hay valor decidido
			%Y responderle con el valor decidido de la instancia
			{ExisteEstaInstancia, ValorAhora} = paxos:estado(node(), NumInstancia),
			if ExisteEstaInstancia ->
				io:format("Va atrasado  ~p , ~p~n",[NodoFrom, {NumInstancia,ValorAhora}]),
				{paxos, NodoFrom} ! {decidido, {NumInstancia, ValorAhora}},
				aceptadorUnaInstancia(Servidores, NumeroInstancia, N_p, N_a, V_a);
			N >= N_p->%Gestionar mensaje acepta
				N_pp = N, N_aa = N, V_aa = {NumInstancia, Valor},
				{paxos, NodoFrom} !  {acepta_ok, N, node(), NumInstancia},
				aceptadorUnaInstancia(Servidores, NumeroInstancia, N_pp, N_aa, V_aa);
			true ->
				{paxos, NodoFrom} ! {acepta_reject, N_p, node(), NumInstancia},
				aceptadorUnaInstancia(Servidores, NumeroInstancia, N_p, N_a, V_a)
			end;


		comprobar_estado -> %Comprueba el estado de los valores de otros nodos de paxos, por si vas retrasado
			{ExisteValor, Valor} = paxos:enviarEstado(Servidores, NumeroInstancia),
			io:format("Comprobando el valor de la instancia ~p en el nodo ~p~n",[NumeroInstancia, node()]),
			if ExisteValor ->
				{paxos, node()} ! {decidido, {NumeroInstancia, Valor}},
				io:format("Se actualiza el valor de la instancia ~p en el nodo ~p~n",[NumeroInstancia, node()]);
			true-> sigo_esperando
			end,
			aceptadorUnaInstancia(Servidores, NumeroInstancia, N_p, N_a, V_a);
		
		crear_control -> spawn_link(?MODULE, comprobar_estado_paxos, [self()]),
			aceptadorUnaInstancia(Servidores, NumeroInstancia, N_p, N_a, V_a);

		kill -> termino;

		Mensaje   ->  io:format("Mensaje en aceptador no identificado ~p, ~p ~n",[node(), Mensaje]),
					aceptadorUnaInstancia(Servidores, NumeroInstancia, N_p, N_a, V_a)
	end.

%%Proceso que cada un determinado tiempo avisa a los aceptadores que comprueben el estado de otros servidores
%%por si se han quedado sordos o particionados y vuelven a estar en contacto
comprobar_estado_paxos(Pid) ->

	timer:sleep(?T_ETAPA),
	Pid ! comprobar_estado, 
	comprobar_estado_paxos(Pid).




