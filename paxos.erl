%% ----------------------------------------------------------------------------
%% paxos : Modulo principal Paxos
%%
%% 
%% 
%% 
%% 
%% ----------------------------------------------------------------------------

-module(paxos).

-export([start/3, start_instancia/3, estado/2, hecho/2, max/1, min/1]).
-export([comm_no_fiable/1, limitar_acceso/2, stop/1, vaciar_buzon/0]).
-export([n_mensajes/1]).
-export([ponte_sordo/1, escucha/1]).              
-export([init/2]).
-export([minimasInstanciasNodos/1, calculoMinimo/2, enviarEstado/2 ]).

-define(MODULEACEPTADOR, aceptador).
-define(MODULEPROPONENTE, proponente).
-define(T_ESPERAA, 150).
-define(T_ESPERAB, 60).
-define(T_ETAPA, 2000).
-define(PRINT(Texto,Datos), io:format(Texto,Datos)).
-define(ENVIO(Mensj, Dest),
        io:format("Llega a nodo ~p se envia ~p a ~p~n",[node(), Mensj, Dest]), Dest ! Mensj).
-define(ENVION(Mensj, Dest), Dest ! Mensj).
-define(ESPERO(Dato), Dato -> io:format("LLega ~p-> ~p~n",[Dato,node()]), ).



%% El que invoca a las funciones exportables es el nodo erlang maestro
%%  que es creado en otro programa...para arrancar los nodos réplica (con paxos)

%% - Resto de nodos se arranca con slave:start(Host, Name, Args)
%% - Entradas/Salidas de todos los nodos son redirigidos por Erlang a este nodo
%%       (es el funcionamiento especificado por modulo slave)
%% - Todos los  nodos deben tener el mismo Sist de Fich. (NFS en distribuido ?)
 
%% - Args contiene string con parametros para comando shell "erl"
%% - Para la ejecución en línea de comandos shell de la VM Erlang, 
%%   definir ssh en parámetro -rsh y habilitar authorized_keys en nodos remotos


%%%%%%%%%%%% FUNCIONES EXPORTABLES  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%-----------------------------------------------------------------------------
%% Crear y poner en marcha un servidor Paxos
%% Los nombres Erlang completos de todos los servidores están en Servidores
%% Y el nombre de máquina y nombre nodo Erlang de este servidor están en 
%% Host y NombreNodo
%% Devuelve :  ok.
-spec start( list(node()), atom(), atom() ) -> ok.
start(Servidores, Host, NombreNodo) ->
    Args = "-setcookie palabrasecreta", % args para comando remoto erl
        % arranca servidor en nodo remoto
    {ok, Nodo} = slave:start(Host, NombreNodo, Args),
    io:format("Nodo esclavo en marcha~n",[]),
    process_flag(trap_exit, true),
    spawn_link(Nodo, ?MODULE, init, [Servidores,Nodo]).


%%-----------------------------------------------------------------------------
%% petición de inicio de proceso de acuerdo para una instancia NuInstancia
%% Con valor propuesto Valor.
%% al servidor Paxos : NodoPaxos
%% Devuelve de inmediato:  ok. 
-spec start_instancia( node(), non_neg_integer(), string() ) -> ok.
start_instancia(NodoPaxos, NuInstancia, Valor) ->
	MinimaInstancia=min(NodoPaxos),
	if NuInstancia < MinimaInstancia -> Respuesta=prueba_instancia_mayor;
	true ->
		{paxos, NodoPaxos} ! {start_instancia, NuInstancia, Valor, self()},
       		receive Respuesta -> Respuesta end
	end,
	?PRINT("~p~n",[Respuesta]),Respuesta.




%%-----------------------------------------------------------------------------
%% La aplicación quiere saber si este servidor opina que
%% la instancia NUInstancia ya se ha decidido.
%% Solo debe mirar el servidor NodoPaxos sin contactar con ningún otro
%% Devuelve : {Decidido :: bool, Valor}
-spec estado( node(), non_neg_integer() ) -> { boolean() , string() }.
estado(NodoPaxos, NuInstancia) ->	

	%Preguntar al proceso paxos, si hay una instancia en registro, y si la hay, obtener el valor
	{paxos, NodoPaxos} ! {registroExisteValor, NuInstancia, self()},
        receive 
	{respuesta_reg_estado_valor, ExisteRegistro, [Valor|_] } -> Respuesta={ ExisteRegistro, Valor};
	{respuesta_reg_estado_valor, ExisteRegistro, OtroValor } -> Respuesta={ ExisteRegistro, OtroValor}
	%Si el nodo no responde, suponer que no hay valor
	after ?T_ESPERAA -> Respuesta = {false, sin_valor}
	end,
	Respuesta.



%%-----------------------------------------------------------------------------
%% La aplicación en el servidor NodoPaxos ya ha terminado
%% con todas las instancias <= NuInstancia
%% Mirar comentarios de min() para más explicaciones
%% Devuelve :  ok.
-spec hecho( node(), non_neg_integer() ) -> ok.
hecho(NodoPaxos, NuInstancia) ->
	%Mandar mensaje al nodo paxos de hecho para tener constancia
	{paxos, NodoPaxos} ! {hecho, NuInstancia}, 
	Minimo=min(NodoPaxos),
	%Borrar memoria que pueda borrarse (la que todos los nodos existentes ya no necesitan)
	{paxos, NodoPaxos} ! {borrar_memoria, Minimo-1}, 
	ok.



%%-----------------------------------------------------------------------------
%% Aplicación quiere saber el máximo número de instancia que ha visto
%% este servidor NodoPaxos
% Devuelve : NuInstancia
-spec max( node() ) -> non_neg_integer().
max(NodoPaxos) ->
	%Preguntar al proceso de paxos por la maxima instancia vista
	{paxos, NodoPaxos} ! {maximaInstancia, self()},
        receive {max, Valor } -> Respuesta=Valor
	after ?T_ESPERAB -> Respuesta = 0
	end,
	Respuesta.




%%-----------------------------------------------------------------------------
% Minima instancia vigente de entre todos los nodos Paxos
% Se calcula en función aceptador:modificar_state_inst_y_hechos
%Devuelve : NuInstancia = hecho + 1
-spec min( node() ) -> non_neg_integer().
min(NodoPaxos) ->
	%Preguntar al proceso de paxo el minimo de todo paxos
	{paxos, NodoPaxos} ! {minimaInstanciaNodos, self()},
        receive {minNodos, Valor} -> Respuesta=Valor
	after ?T_ETAPA -> Respuesta = 1
	end,
	%Respuesta= min(hecho())+1
	Respuesta_mas_uno=Respuesta+1,
	io:format("min ~p, respuesta ~p~n",[NodoPaxos,Respuesta_mas_uno]),
	Respuesta_mas_uno.




%%-----------------------------------------------------------------------------
% Calcula el mínimo local es decir, el valor más alto en la historia de la funcion hecho()
minLocal(NodoPaxos) ->
	%Pregunta al nodo por el mínimo local
	{paxos, NodoPaxos} ! {minimaInstancia, self()},
        receive {min, Valor} -> Respuesta=Valor
	after ?T_ESPERAA -> Respuesta = 1
	end,
	Respuesta.


%%-----------------------------------------------------------------------------
% Cambiar comportamiento de comunicación del Nodo Erlang a NO FIABLE
-spec comm_no_fiable( node() ) -> no_fiable.
comm_no_fiable(Nodo) ->
    {paxos, Nodo} ! no_fiable.

    
    
%%%%%%%%%%%%%%%%%%
%% Limitar acceso de un Nodo a solo otro conjunto de Nodos,
%% incluido este nodo de control
%% Para simular particiones de red
-spec limitar_acceso( node(), list(node()) ) -> ok.
limitar_acceso(Nodo, Nodos) ->
    {paxos, Nodo} ! {limitar_acceso, Nodos ++ [node()]}.

    
    
%%-----------------------------------------------------------------------------
%% Hacer que un servidor Paxos deje de escuchar cualquier mensaje salvo 'escucha'
-spec ponte_sordo( node() ) -> ponte_sordo.
ponte_sordo(NodoPaxos) ->
    {paxos, NodoPaxos} ! ponte_sordo.



%%-----------------------------------------------------------------------------
%% Hacer que el servidor Paxos vuelva a recibir todos los mensajes normales
-spec escucha( node() ) -> escucha.
escucha(NodoPaxos) ->
    {paxos, NodoPaxos} ! escucha.



%%-----------------------------------------------------------------------------
%% Parar nodo Erlang remoto
-spec stop( node() ) -> ok.
stop(NodoPaxos) ->
    slave:stop(NodoPaxos),
    vaciar_buzon().
    
%%-----------------------------------------------------------------------------
% Vaciar buzon de un proceso, tambien llamado en otros sitios flush()
-spec vaciar_buzon() -> ok.
vaciar_buzon() ->
    receive _ -> vaciar_buzon()
    after   0 -> ok
    end.


%%-----------------------------------------------------------------------------
%% Obtener numero de mensajes recibidos en un nodo
-spec n_mensajes( node() ) -> non_neg_integer().
n_mensajes(NodoPaxos) ->
        {paxos, NodoPaxos} ! {n_mensajes, self()},
        receive Respuesta -> Respuesta end.
    
    
    
    
    
%%%%%%%%%%%%%%%%%  FUNCIONES LOCALES  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



%%Se inicializa el servidor Paxos
init(Servidores, Yo) ->
	io:format("Nodo inicializado ~p~n",[node()]),
	register(paxos, self()),
	%%Se crea los datos que maneja paxos y el proceso proponente y acpetador
	BDInstancias = datos_paxos:crearRegistro(Yo),
	PidProponente = spawn_link(Yo, ?MODULEPROPONENTE, proponente, [Yo, Servidores,  dict:new()]),
	PidAceptador = spawn_link(Yo, ?MODULEACEPTADOR, aceptador, [Yo, Servidores, dict:new()]),
	%Se registran
	register(proponente, PidProponente),
	register(aceptador, PidAceptador),
	bucle_recepcion(Servidores, Yo, BDInstancias).

  

%%-----------------------------------------------------------------------------
bucle_recepcion(Servidores, Yo, BDInstancias) ->

    receive
	{start_instancia, NuInstancia, Valor, From} ->
		ExisteInstanciaProponente = datos_paxos:existeInstanciaProponentes(BDInstancias, NuInstancia),
		ExisteInstanciaAceptador = datos_paxos:existeInstanciaAceptadores(BDInstancias, NuInstancia),
		ExisteRegistro = datos_paxos:existeRegistro(BDInstancias, NuInstancia),
		
		if ExisteInstanciaProponente or ExisteInstanciaAceptador or ExisteRegistro-> 
			%Ya hay proponente
			From ! ya_existe_proponente,
			%Ya había proponentes y no tienes el registro, preguntarlo
			{ExisteValor, ValorNuevo} = enviarEstado(Servidores, NuInstancia),
			if ExisteValor and not ExisteRegistro ->
				{paxos, Yo} ! {decidido, {NuInstancia, ValorNuevo}};
			  true -> nada
			end,
			bucle_recepcion(Servidores, Yo, BDInstancias);

		%start instancia
		true -> From ! ok,
			{proponente, Yo}  ! {instancia, NuInstancia, Valor},
			BDnewnew = datos_paxos:mensajeAdd(BDInstancias),
			BDnewOtra = datos_paxos:intentarSetInstanciaMaximaVista(BDnewnew, NuInstancia),
			bucle_recepcion(Servidores, Yo, BDnewOtra)
		end;


	{decidido, {NuInstancia, Valor}} -> 
		%Guardar el valor
		ExisteRegistro=datos_paxos:existeRegistro(BDInstancias, NuInstancia),
		if  ExisteRegistro-> %Si ya existe no guardar
			 BDnew=BDInstancias;	
		true ->  BDnew=datos_paxos:addRegistro(BDInstancias, {NuInstancia, Valor}),	  
			?PRINT("Decidida instancia ~p en nodo: ~p~n",[{NuInstancia, Valor}, node()])
		end,
		BDnewOtra = datos_paxos:intentarSetInstanciaMaximaVista(BDnew, NuInstancia),
		bucle_recepcion(Servidores, Yo, BDnewOtra);

	{registroExiste, NumInstancia, From, Estado} ->
		%Devuelve un booleano correspondiente a si para ese numero de instancia, se tiene valor en el registro
		 ExisteRegistro=datos_paxos:existeRegistro(BDInstancias, NumInstancia),
	         From ! {respuesta_reg_estado, ExisteRegistro, Estado},
   		 BDnewnew = datos_paxos:mensajeAdd(BDInstancias),
		 bucle_recepcion(Servidores, Yo, BDnewnew);

	{registroExisteValor, NumInstancia, From} ->
		%Devuelve el valor correspondiente de una instancia, se tiene valor en el registro
		 ExisteRegistro = datos_paxos:existeRegistro(BDInstancias, NumInstancia),
		 if ExisteRegistro -> Valor = datos_paxos:getRegistroValor(BDInstancias, NumInstancia);
			true-> 	Valor = sin_valor %No hay valor en el registro
			end,
	         From ! {respuesta_reg_estado_valor, ExisteRegistro, Valor},
		 bucle_recepcion(Servidores, Yo, BDInstancias);


	{maximaInstancia, Pid} -> %Devuelve el numero de instancia maxima vista
		Pid ! {max, datos_paxos:getInstanciaMaximaVista(BDInstancias)},
		bucle_recepcion(Servidores, Yo, BDInstancias);

	{minimaInstancia, Pid} -> %Devuelve el numero de instancia minima (maxima de funcion hecho())
		Pid ! {min, datos_paxos:getInstanciaMinima(BDInstancias)},
		bucle_recepcion(Servidores, Yo, BDInstancias);

	{minimaInstanciaNodos, Pid} ->
		%Crea un proceso que calculará y enviará a Pid el número minimo de instancias
		%De entre todos los servidores paxos
		spawn_link(Yo, ?MODULE, calculoMinimo, [Servidores, Pid]),
		bucle_recepcion(Servidores, Yo, BDInstancias);


	{hecho, NuInstancia} ->	%Edita el valor de valiable minima hecho, que no se vovlerá a utilizar
		BDnew = datos_paxos:setInstanciaMinima(BDInstancias, NuInstancia),
                bucle_recepcion(Servidores, Yo, BDnew);

	{borrar_memoria, Minimo} ->%Eliminar memoria solo con el minimo de todos, procesos relacionados, aceptador y proponentes
		BDnewnew = eliminarMemoria(Minimo, BDInstancias),
                bucle_recepcion(Servidores, Yo, BDnewnew);

        no_fiable    ->%Edita varaible de fiable = false
		BDnew = datos_paxos:setFiabilidad(BDInstancias,false),
		bucle_recepcion(Servidores, Yo, BDnew);
       
        fiable       -> %Edita varaible de fiable = true
		BDnew = datos_paxos:setFiabilidad(BDInstancias,true),
                bucle_recepcion(Servidores, Yo, BDnew);

 	{getBD, Pid}    -> %Devuelve la BDinstancias al Pid
		Pid ! BDInstancias,
		bucle_recepcion(Servidores, Yo, BDInstancias);

	{getInstanciasAceptadores, Estado, Pid, TipoMensaje}   -> 
		%Devuelve el diccionaro de cada instancia  y el Pid de su aceptador
		Pid ! {instanciasAceptadores, TipoMensaje, datos_paxos:getInstanciasAceptadores(BDInstancias), Estado},
		BDnewnew = datos_paxos:mensajeAdd(BDInstancias),
		bucle_recepcion(Servidores, Yo, BDnewnew);

        {addInstanciaProponente, Instancia, Pid}       ->   
		%Devuelve el diccionaro de cada instancia  y el Pid de su proponente
		BDnew = datos_paxos:addInstanciaProponente(BDInstancias, {Instancia, Pid}),
	      	bucle_recepcion(Servidores, Yo, BDnew);

        {addInstanciaAceptadores, Instancia, Pid}       ->  
		BDnew = datos_paxos:addInstanciaAceptador(BDInstancias, {Instancia, Pid}),
	      	bucle_recepcion(Servidores, Yo, BDnew);

        {es_fiable, Pid} -> %Devuelve el valor de fiable
		Pid ! datos_paxos:getFiabilidad(BDInstancias),
		bucle_recepcion(Servidores, Yo, BDInstancias);

        {limitar_acceso, Nodos} ->  % Limita acceso a solo Nodos
	io:format("En nodo ~p solo se acepta a ~p~n",[node(), Nodos]),
		error_logger:tty(false), 
		net_kernel:allow(Nodos),
		bucle_recepcion(Servidores, Yo, BDInstancias);
       
        {n_mensajes, Pid} ->%Devuelve numero de mensajes enviados en el algoritmo hasta el momento
		Num = datos_paxos:getNumMensajes(BDInstancias),
		Pid ! Num,
		bucle_recepcion(Servidores, Yo, BDInstancias);
                    
        ponte_sordo -> espero_escucha(Servidores, Yo, BDInstancias);
        
        {'EXIT', _Pid, _DatoDevuelto} -> %%Cuando proceso proponente acaba
		adios;
             
            %% mensajes para proponente y aceptador del servidor local
        Mensajes_prop_y_acept ->
		simula_fallo_mensj_prop_y_acep(Servidores, Yo, Mensajes_prop_y_acept, BDInstancias, datos_paxos:getFiabilidad(BDInstancias))
    end.
    
%%-----------------------------------------------------------------------------
simula_fallo_mensj_prop_y_acep(Servidores, Yo, Mensaje, BDInstancias, Es_fiable) ->

    Aleatorio = random:uniform(1000),
      %si no fiable, eliminar mensaje con cierta aleatoriedad
    if  ((not Es_fiable) and (Aleatorio < 200)) -> 
                bucle_recepcion(Servidores, Yo, BDInstancias);
                  % Y si lo es tratar el mensaje recibido correctamente
        true -> gestion_mnsj_prop_y_acep(Servidores, Yo, Mensaje, BDInstancias)
    end.


%%-----------------------------------------------------------------------------
% implementar tratamiento de mensajes recibidos en  Paxos
% Tanto por proponentes como aceptadores
gestion_mnsj_prop_y_acep(Servidores, Yo, Mensaje, BDInstancias) ->
    % Mensajes que puede recibir el aceptador {prepara,N} {acepta,N,V}
    % Mensajes que puede recibir el proponente {prepara_ok,N,Na,Va} {acepta_ok,N}
	BDnewnew = datos_paxos:mensajeAdd(BDInstancias),
	case Mensaje of
	{prepara, _N, Nodo, NumInstancia}  ->
		?PRINT("Prepara llega a: ~p de:~p~n",[node(), Nodo]),
		 BDnewOtra = datos_paxos:intentarSetInstanciaMaximaVista(BDnewnew, NumInstancia),
		 aceptador ! Mensaje;
	{acepta, {NuInst, _Valor}, _N, Nodo} ->
		?PRINT("acepta llega a: ~p de:~p~n",[node(), Nodo]),
		 BDnewOtra = datos_paxos:intentarSetInstanciaMaximaVista(BDnewnew, NuInst),
		 aceptador ! Mensaje;
	{prepara_ok,_N,_Na,_Va, Nodo, _NumInstancia}  ->
		?PRINT("Ok_prepara llega a: ~p de:~p~n",[node(), Nodo]),
		 BDnewOtra = BDnewnew,
		 {proponente, Yo}  ! Mensaje;
	{acepta_ok, _N, Nodo, _NumInstancia}  ->
		?PRINT("Ok_acepta llega a: ~p de:~p~n",[node(), Nodo]),
		 BDnewOtra = BDnewnew,
		 {proponente, Yo}  ! Mensaje;
	{acepta_reject, _Np, _Nodo, _NumInstancia}  ->
		 BDnewOtra = BDnewnew,
		 {proponente, Yo}  ! Mensaje;
	{prepara_reject, _Np, _Nodo, _NumInstancia}  ->
		 BDnewOtra = BDnewnew,
		 {proponente, Yo}  ! Mensaje;

	_ ->	BDnewOtra = BDnewnew,
		?PRINT("Recibido menasje no identificado: ~p~n",[Mensaje])
	end,

	bucle_recepcion(Servidores, Yo, BDnewOtra).


%%-----------------------------------------------------------------------------
espero_escucha(Servidores, Yo, BDInstancias) ->
    io:format("~p : Esperando a recibir escucha~n",[node()]),
    receive
        escucha ->
            io:format("~p : Salgo de la sordera !!~n",[node()]),
            bucle_recepcion(Servidores, Yo, BDInstancias);
        Resto -> io:format("ESTOYSORDO ~p: ~p ~n",[Resto, node()]),espero_escucha(Servidores, Yo, BDInstancias)
    end.




%% Pregunta a todos los servidores el estado de una instancia, y devuelve si existe valor
%% y el valor si existe, es decir, devuelve para una instancia el estado en todo paxos
enviarEstado(Servidores, NuInstancia)-> 
	if Servidores==[] -> Respuesta={false, sin_valor};
	true ->
		[S|OtrosServidores]=Servidores,
		{Existe, ValorReal} = estado(S, NuInstancia),
		if Existe ->% devolver ya el valor de la instancia
			Respuesta={Existe, ValorReal};
		true -> %no hay valor para ese servidor, seguir buscando en otros
			Respuesta=enviarEstado(OtrosServidores, NuInstancia)
		end
	end,
	Respuesta.

%% devuelve la lista de todos las minimas minLocal() de entre todos los nodos
minimasInstanciasNodos(Servidores)->

    	case Servidores of
		[] ->Respuesta=[];
		[S|OtrosServidores]->
			[S|OtrosServidores]=Servidores,
			Valor = minLocal(S),
			Respuesta=[Valor] ++ minimasInstanciasNodos(OtrosServidores)
	end,
	Respuesta.


%% Devuelve la BD nueva, después de eliminar las instancias menores o iguales a N, y sus proceso correspondientes 
eliminarMemoria(N, BDInstancias)->

	EsInstanciaDecidida = dict:is_key(N, datos_paxos:getRegistro(BDInstancias)),
	if EsInstanciaDecidida ->% Si existe la instancia borrarla
		io:format("Borrando instancia ~p de nodo ~p~n",[N,node()]),
		BDNew=datos_paxos:borrarEntradaRegistro(N, BDInstancias),
		Existe = dict:is_key(N,datos_paxos:getInstanciasAceptadores(BDNew)),
		if Existe -> % Si existe el proceso aceptador borrarlo
			{ok, [PidAceptador|_]} = dict:find(N, datos_paxos:getInstanciasAceptadores(BDNew)),
			PidAceptador ! {kill};
		true -> ya_Esta_muerto
		end,
		ExisteP = dict:is_key(N, datos_paxos:getInstanciasProponentes(BDNew)),
		if ExisteP -> % Si existe el proceso proponente borrarlo
			{ok, [PidProponente|_]} = dict:find(N, datos_paxos:getInstanciasProponentes(BDNew)),
			PidProponente ! {kill};

		true -> ya_Esta_muerto
		end,
		%Seguir borrando 
		Respuesta=eliminarMemoria(N-1, BDNew);

	true->%ya no hay que borrar mas, devolver BD
		Respuesta=BDInstancias
	end,
	Respuesta.


%% Calcula el minimo de minLocal de entre todos los servidores y contesta al Pid con el resultado
calculoMinimo(Servidores, Pid)->
		ListaMinimos = minimasInstanciasNodos(Servidores),
		Menor = lists:min(ListaMinimos),
		io:format("Menor total ~p de nodo ~p~n",[Menor,node()]),
		Pid ! {minNodos, Menor}.


