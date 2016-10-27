%% ----------------------------------------------------------------------------
%% paxos : Modulo proponente paxos
%%
%% 
%% 
%% 
%% 
%% ----------------------------------------------------------------------------

-module(proponente).


-export([proponente/3, proponenteUnaInstancia/4]).

-define(T_ESPERAA, 150).
-define(T_ESPERAB, 60).
-define(T_ETAPA, 2000).
-define(PRINT(Texto,Datos), io:format(Texto,Datos)).
-define(ENVIO(Mensj, Dest),
        io:format("Llega a nodo ~p se envia ~p a ~p~n",[node(), Mensj, Dest]), Dest ! Mensj).
-define(ENVION(Mensj, Dest), Dest ! Mensj).
-define(ESPERO(Dato), Dato -> io:format("LLega ~p-> ~p~n",[Dato,node()]), ).




%%%%%%%%%%%% FUNCIONES EXPORTABLES  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


    
    
%%Proceso que gestiona todos los proponentes del nodo para cada instancia
proponente(Yo, Servidores, ListaProponentes)->

	receive
		%Llega petición de iniciar una instancia, se crea un proceso para esa instancia
		{instancia, NumInstancia, Valor} -> 
			 %Crea el proceso, lo añade a la lista de proponentes y crea un proceso que lo avisa cada cierto tiempo
			 %por si la rona no avanza y necesita reiniciarse (debido a perdida de mensajes)
			 Proc = spawn_link(Yo, ?MODULE, proponenteUnaInstancia, [Yo,{NumInstancia,Valor, 0, 0, 0, 0,-1 }, 0, Servidores]),
			 Proc ! empieza,
			 NuevaListaListaProponentes = dict:append(NumInstancia, Proc, ListaProponentes),
			 paxos ! {addInstanciaProponente, NumInstancia, Proc},
			 timer:send_interval(1000, Proc, sino_mayoria_reinicia),
			 proponente(Yo, Servidores, NuevaListaListaProponentes);
			  

		{prepara_ok,N,Na,Va, Nodo, NumInstancia}  ->%Reenvia al proceso de la instancia correspondiente
			{ok, [PidProponente|_]} = dict:find(NumInstancia, ListaProponentes),
			?ENVION({prepara_ok,N,Na,Va, Nodo}, PidProponente),
			proponente(Yo, Servidores, ListaProponentes);

		{acepta_ok,N, Nodo, NumInstancia}  ->%Reenvia al proceso de la instancia correspondiente	
			{ok, [PidProponente|_]} = dict:find(NumInstancia, ListaProponentes),
			?ENVION({acepta_ok,N, Nodo}, PidProponente),
			proponente(Yo, Servidores, ListaProponentes);

		{acepta_reject, Np, Nodo, NumInstancia}  ->%Reenvia al proceso de la instancia correspondiente
			{ok, [PidProponente|_]} = dict:find(NumInstancia, ListaProponentes),
			?ENVION({acepta_reject, Np, Nodo}, PidProponente),
			proponente(Yo, Servidores, ListaProponentes);

		{prepara_reject, Np, Nodo, NumInstancia}  ->%Reenvia al proceso de la instancia correspondiente 
			{ok, [PidProponente|_]} = dict:find(NumInstancia, ListaProponentes),
			?ENVION({prepara_reject, Np, Nodo}, PidProponente),
			proponente(Yo, Servidores, ListaProponentes);

		Mensaje   ->
			io:format("Mensaje en proponente no identificado ~p, ~p ~n",[Yo, Mensaje]),
			proponente(Yo, Servidores, ListaProponentes)
	end.



%Proceso proponente para una instancia
proponenteUnaInstancia(Yo, InstanciaProponiendo, N, Servidores)->
	%Variables a controlar del proponente
	{NumInstancia, Valor, Prepara_oks, Acepta_oks, Prepara_rejects, Acepta_rejects, N_a_mayor} = InstanciaProponiendo,

	receive
		empieza ->
			io:format("Se propone ~p en ~p ~n",[{NumInstancia, Valor},node()]),
			enviarToAceptadoresPrepara(Servidores, N, Yo, NumInstancia), %Enviar prepara a todos los acpetadores
			proponenteUnaInstancia(Yo, InstanciaProponiendo, N, Servidores);

		{prepara_ok, N, N_a, V_a, _NodoFrom} ->

			Prepara_oks_nuev = Prepara_oks+1,
			io:format("Proponente nodo  ~p  con ~p preparados ~n",[node(), Prepara_oks_nuev]),
			if N_a > N_a_mayor ->%Mirar si actualizar valor
				ValorNuevo = V_a;

			true ->	ValorNuevo = Valor
			end,
			HayMayoria = mayoria(Prepara_oks_nuev, Servidores),
			%Comprobar si hay mayoría
			if HayMayoria ->
				io:format("Hay mayoría prepara ~p en node=~p con ~p ~n",[{NumInstancia, Valor},node(), Prepara_oks_nuev]),
				%Enviar prepara a todos los aceptadores 
				enviarToAceptadoresAcepta({NumInstancia, ValorNuevo}, Servidores, N, Yo), 
				ValoresEstado = {NumInstancia, ValorNuevo, 0, Acepta_oks, Prepara_rejects, Acepta_rejects, N_a_mayor},
				proponenteUnaInstancia(Yo, ValoresEstado, N, Servidores);

			true -> 
			ValoresEstado = {NumInstancia, ValorNuevo, Prepara_oks_nuev, Acepta_oks, Prepara_rejects, Acepta_rejects, N_a_mayor},
			proponenteUnaInstancia(Yo, ValoresEstado, N, Servidores)

		end;

		{prepara_reject, N_p, _NodoFrom} ->
			%No aceptar rejects de rondas anteriores
			if N_p >= N -> Mas_Prepara_rejects = Prepara_rejects+1;
				true-> Mas_Prepara_rejects = Prepara_rejects
			end,
			HayMayoria = mayoria(Mas_Prepara_rejects, Servidores),
			 if HayMayoria -> io:format("Hay mayoría prepara_reject ~p en node=~p ~n",[{NumInstancia, Valor},node()]), 
				%Reiniciar el algoritmo con N mas grande
				EstadoaReiniciar = {Yo, {NumInstancia, Valor,0, 0, 0, 0, -1}, N_p+1, Servidores},
				io:format("Intento: Reinicio de instancia ~p~n",[Yo]),
				%Preguntar antes de reiniciar si se ha tomado consenso para esa instancia y no te has enterado
				{paxos, Yo} ! {registroExiste, NumInstancia, self(), EstadoaReiniciar},
				proponenteUnaInstancia(Yo, InstanciaProponiendo, N, Servidores);

			   true -> 
				ValoresEstado = {NumInstancia, Valor, Prepara_oks, Acepta_oks, Mas_Prepara_rejects, Acepta_rejects, N_a_mayor},
				proponenteUnaInstancia(Yo, ValoresEstado, N, Servidores)
			 end;
					 

		{acepta_ok, N, _NodoFrom} -> 
			io:format("Proponente nodo  ~p , acepta_ok ~p con ~p aceptados ~n",[node(), {NumInstancia, Valor}, Acepta_oks+1]),
			Mas_Acepta_oks = Acepta_oks+1,
			HayMayoria = mayoria(Mas_Acepta_oks, Servidores),
			if HayMayoria -> io:format("Hay mayoría acepta ~p en node=~p con ~p~n",[{NumInstancia, Valor},node(), Mas_Acepta_oks]),
				%Enviar decidido a todos los otros nodos 
				enviarToAprendiz({NumInstancia, Valor}, Servidores);
			   true -> 
				ValorEstado =  {NumInstancia, Valor, Prepara_oks, Mas_Acepta_oks, Prepara_rejects, Acepta_rejects, N_a_mayor},
				proponenteUnaInstancia(Yo, ValorEstado, N, Servidores)
			end;



		{acepta_reject, N_p, _NodoFrom} -> io:format("Proponente nodo  ~p , acepta_reject ~p  ~n",[node(), {NumInstancia, Valor}]),
 			%No aceptar rejects de rondas anteriores
			if N_p >= N ->  Mas_Acepta_rejects = Acepta_rejects+1;
				true->  Mas_Acepta_rejects = Acepta_rejects
			end,
			 HayMayoria = mayoria(Mas_Acepta_rejects, Servidores),
			 if HayMayoria -> io:format("Hay mayoría acepta_reject ~p en node=~p ~n",[{NumInstancia, Valor},node()]), 
				%Reiniciar el algoritmo con N mas grande
				EstadoaReiniciar = {Yo, {NumInstancia, Valor,0, 0, 0, 0, -1}, N_p+1, Servidores},
				io:format("Intento: Reinicio de instancia ~p~n",[Yo]),
				%Preguntar antes de reiniciar si se ha tomado consenso para esa instancia y no te has enterado
				{paxos, Yo} ! {registroExiste, NumInstancia, self(), EstadoaReiniciar},
				proponenteUnaInstancia(Yo, InstanciaProponiendo, N, Servidores);

			   true -> 
				ValorEstado =  {NumInstancia, Valor, Prepara_oks, Acepta_oks, Prepara_rejects, Mas_Acepta_rejects, N_a_mayor},
				proponenteUnaInstancia(Yo, ValorEstado, N, Servidores)
			 end;


		{respuesta_reg_estado, ExisteRegistro, Estado} ->
			{Yoo,EstadoReinicioo , Nn, Servidoress}=Estado,
			
			if ExisteRegistro -> %Si existe en tu registro no reiniciar
				io:format("No se reinicia instancia por tener ya el registro con ese numero ~p~n",[Yoo]);
			  true ->
				%si no tienes el registro, preguntar a los demas servidores a ver
				{ExisteEnOtros,ValorEnOtros}=paxos:enviarEstado(Servidoress, NumInstancia),
				if ExisteEnOtros -> %Si existe en algun nodo paxos guardarte el valor
					{paxos, Yoo} ! {decidido, {NumInstancia, ValorEnOtros}},
					io:format("No se reinicia instancia por tener ya el registro con ese numero ~p~n",[Yoo]);
				true->
					io:format("Hecho el reinicio, nodo ~p~n",[Yoo]),
					self() ! empieza,
					proponenteUnaInstancia(Yoo, EstadoReinicioo, Nn, Servidoress)
				end
			end;

		sino_mayoria_reinicia ->
			%Cada cierto tiempo llegara este mensaje para comprobar que no se ha decidido	
			%la instancia en paxos y tu no te has enterado	
			EstadoaReiniciar = {Yo, {NumInstancia, Valor,0, 0, 0, 0, -1}, N+1, Servidores},
			io:format("Intento: Reinicio de instancia ~p~n",[Yo]),
			{paxos, Yo} ! {registroExiste, NumInstancia, self(), EstadoaReiniciar},
			proponenteUnaInstancia(Yo, InstanciaProponiendo, N, Servidores);

		kill -> termino;

		Algo   -> io:format("Mensaje en proponente no identificado ~p, ~p ~n",[Yo, Algo]),
			  proponenteUnaInstancia(Yo, InstanciaProponiendo, N, Servidores)

	end.


%Devuelve el tamano de la lista
num([]) -> 0;
num(NUMS) ->
        num(NUMS, 0).

num([_H|L], Count) ->
        num(L, Count+1);
num([], Count) ->
        Count.

%Devuelve true si num_oks un numero mayor de la mitad del tamano de Servidores 
mayoria(Num_oks, Servidores)->
	num(Servidores)<(Num_oks*2).

%% Envia mensaje prepara a todos los aceptadores
enviarToAceptadoresPrepara(Servidores, N, _Yo, NumInstancia)-> 
    lists:foreach(fun(S) ->  {paxos, S} ! {prepara, N, node(), NumInstancia}  end, Servidores ).

%% Envia mensaje acepta a todos los aceptadores
enviarToAceptadoresAcepta(V, Servidores, N, Yo)-> 
    lists:foreach(fun(S) ->  {paxos, S} ! {acepta, V, N, Yo}  end, Servidores ).

%% Envia mensaje decidido a todos los nodos paxos
enviarToAprendiz(V, Servidores)-> 
    lists:foreach(fun(S) ->  {paxos, S} ! {decidido, V}, ?PRINT("Decidido enviado a: ~p~n",[S])   end, Servidores ).



