-module(paxos_tests).
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(HOST, '127.0.0.1').

-define(T_ESPERA, 20).


%%%%%%%%%%%%%%%%%%%% FUNCIONES DE APOYO  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% devueve lista de nombres de servidores a aprtir de lista númerica
servidores(Lista_Num) ->
    [ list_to_atom(lists:concat([n, X, '@', ?HOST]) ) || X <- Lista_Num ].
    
    
%%%%%%%%%%%%%%%%%
iguales([]) -> ok;
iguales([ _A | [] ]) -> ok;
iguales([PrimerValor | RestoValoresDecid]) ->
    lists:foldl(fun(X,Previo) -> 
                    if X =:= Previo -> X;
                        true -> %% 2 valores no coinciden !!!!
                        exit("Valores decididos no coinciden !")
                    end
                end,
                PrimerValor, RestoValoresDecid).


%%%%%%%%%%%%%%%%%%%
num_decididos(Servidores, NumInstancia) -> 
    io:format("TEST: num_decididos para instancia ~p... ~n",[NumInstancia]),
    ListParDec =[paxos:estado(Serv, NumInstancia) || Serv <- Servidores],
     io:format("TEST: num_decididos para instancia ~p...Lista~p ~n",[NumInstancia, ListParDec]),
     ListDecid = [ V || {true, V} <- ListParDec],
         
     %% todos los valores decididos deben ser idénticos
     iguales(ListDecid),

      % si lo valores ha sido iguales, cuantos ha sido decididos ?  
    length(ListDecid).
    
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Hay que afinar....no funciona bien
esperar_aun_mas_tiempo(Servidores, NumInstancia, NumDeseados, Tiempo, Iter) ->
    timer:sleep(Tiempo),  %% en milisegundos
    
    NuDecididos = num_decididos(Servidores, NumInstancia),
    
    if  NuDecididos < NumDeseados ->
            if  Tiempo < 1000   ->
                    esperar_aun_mas_tiempo(Servidores, NumInstancia,
                                            NumDeseados, Tiempo * 2, Iter +1 );
                Tiempo >= 1000  ->
                    if  Iter < 15   ->
                            esperar_aun_mas_tiempo(Servidores, NumInstancia,
                                            NumDeseados, Tiempo, Iter + 1);
                        Iter >= 15  -> % Ya ha pasado mucho tiempo
                            exit("Han decidido MENOS de deseados")
                    end
            end;
        true -> ok
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
esperar_n_nodos(Servidores, NumInstancia, N_Deseados) ->
    
    timer:sleep(200),  %% en milisegundos
    io:format("TEST: esperando ~p nodos, para instancia ~p ~n",[N_Deseados, NumInstancia]),
    NuDecididos = num_decididos(Servidores, NumInstancia),

    if  NuDecididos < N_Deseados ->
	    io:format("TEST: esperando otra vez ~p nodos, para instancia ~p ~n",[N_Deseados, NumInstancia]),
           esperar_aun_mas_tiempo(Servidores, NumInstancia, N_Deseados, 20, 1);
        true -> io:format("TEST: Listo, estan listos ~p , para instancia ~p ~n",[NuDecididos, NumInstancia]),ok
    end.
    
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
esperar_mayoria(Servidores, NumInstancia) ->
    esperar_n_nodos(Servidores, NumInstancia, length(Servidores) div 2 + 1).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
comprobar_max(Servidores, NumInstancia, Max) ->
    timer:sleep(2000),
    
    N_decid = num_decididos(Servidores, NumInstancia),
    if N_decid > Max ->
            ?debugFmt("Demasiados decididos: N_inst: ~p, N_deci: ~p, Max: ~p~n",
                       [NumInstancia, N_decid, Max]),
            exit("Demasiados decididos");
       true -> ok
    end.


%%%%%%%%%%%%%%%%%%%  FUNCIONES DE TEST %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%
unico_proponente() ->
    ?debugFmt("Test: Unico proponente ...~n",[]),
    
    % Crear 3 nodos Paxos
    Servidores = ['n1@127.0.0.1', 'n2@127.0.0.1', 'n3@127.0.0.1'],
    lists:foreach(fun(Num) -> paxos:start(Servidores, '127.0.0.1', "n" ++ Num)
                  end, ["1", "2", "3"] ),
 
    timer:sleep(?T_ESPERA),
     
    % solicitar la ejecución de una instancia
    paxos:start_instancia('n1@127.0.0.1', 1, "hello"),
        
    % A esperar a que decidan el mismo valor todos
    esperar_n_nodos(Servidores, 1, length(Servidores)),
    
    % parar VMs Erlang
    lists:foreach(fun(S) -> paxos:stop(S) end, Servidores ),
                  
    ?debugFmt(".... Superado~n",[]).


%%%%%%%%%%%%%%%
varios_propo_un_valor() -> 
    ?debugFmt("Test: Varios propo., un valor ...~n",[]),
    
    % Crear 3 nodos Paxos
    Servidores = ['n1@127.0.0.1', 'n2@127.0.0.1', 'n3@127.0.0.1'],
    lists:foreach(fun(Num) -> paxos:start(Servidores, '127.0.0.1', "n" ++ Num)
                  end, ["1", "2", "3"] ),
 
    timer:sleep(?T_ESPERA),
                   
    paxos:start_instancia('n1@127.0.0.1', 1, "hello"),
    paxos:start_instancia('n2@127.0.0.1', 1, "hello"),
    paxos:start_instancia('n3@127.0.0.1', 1, "hello"),
            
    % A esperar a que decidan el mismo valor todos
    esperar_n_nodos(Servidores, 1, length(Servidores)),
    
    % parar VMs Erlang
    lists:foreach(fun(S) -> paxos:stop(S) end, Servidores ),

    ?debugFmt(".... Superado~n",[]).


%%%%%%%%%%%%%%%
varios_propo_varios_valores() -> 
    ?debugFmt("Test: Varios propo., varios valor ...~n",[]),
    
    % Crear 3 nodos Paxos
    Servidores = ['n1@127.0.0.1', 'n2@127.0.0.1', 'n3@127.0.0.1'],
    lists:foreach(fun(Num) -> paxos:start(Servidores, '127.0.0.1', "n" ++ Num)
                  end, ["1", "2", "3"] ),

    timer:sleep(?T_ESPERA),
                   
    paxos:start_instancia('n1@127.0.0.1', 1, "cuatro"),
    paxos:start_instancia('n2@127.0.0.1', 1, "dos"),
    paxos:start_instancia('n3@127.0.0.1', 1, "tres"),
                      
    % A esperar a que decidan el mismo valor todos
    esperar_n_nodos(Servidores, 1, length(Servidores)),
    
    % parar VMs Erlang
    lists:foreach(fun(S) -> paxos:stop(S) end, Servidores ),
                  
    ?debugFmt(".... Superado~n",[]).


%%%%%%%%%%%%%%%
instancias_fuera_orden() -> 
    ?debugFmt("Test: Instancias fuera de orden..~n",[]),


    % Crear 3 nodos Paxos
    NumServidores = 3,
    Servidores = ['n1@127.0.0.1', 'n2@127.0.0.1', 'n3@127.0.0.1'],
    lists:foreach(fun(Num) -> paxos:start(Servidores, '127.0.0.1', "n" ++ Num)
                  end, ["1", "2", "3"] ),
 
    timer:sleep(?T_ESPERA),
 
    paxos:start_instancia('n1@127.0.0.1', 7, 700),
    paxos:start_instancia('n1@127.0.0.1', 6, 600),
    paxos:start_instancia('n2@127.0.0.1', 5, 500),
    esperar_n_nodos(Servidores, 7, NumServidores),
    paxos:start_instancia('n1@127.0.0.1', 4, 400),
    paxos:start_instancia('n2@127.0.0.1', 3, 300),
    esperar_n_nodos(Servidores, 6, NumServidores),
    esperar_n_nodos(Servidores, 5, NumServidores),
    esperar_n_nodos(Servidores, 4, NumServidores),
    esperar_n_nodos(Servidores, 3, NumServidores),
    
    MaxNuInst = paxos:max('n1@127.0.0.1'),
    if MaxNuInst =/= 7 -> ?debugFmt("max(~p) erróneo, debería ser 7 y es ~p~n",['n1@127.0.0.1', MaxNuInst]);
       true -> ok %MaxNuInst == 7 
    end,
    
    % parar VMs Erlang
    lists:foreach(fun(S) -> paxos:stop(S) end, Servidores ),
                  
    ?debugFmt(".... Superado~n",[]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
proponentes_sordos() ->
    ?debugFmt("Test: Proponentes sordos..~n",[]),

    % Crear 5 nodos Paxos
    NumServidores = 5,
    L_N = lists:seq(1,NumServidores),
    S = servidores(L_N),
    lists:foreach(fun(X) -> paxos:start(S,?HOST,lists:concat([n,X])) end, L_N),
 
    timer:sleep(?T_ESPERA),
    
     paxos:start_instancia('n1@127.0.0.1', 1, "Buenas"), 
    esperar_n_nodos(S, 1, NumServidores),
     
    paxos:ponte_sordo('n1@127.0.0.1'),
    paxos:ponte_sordo('n5@127.0.0.1'),
  
    paxos:start_instancia('n2@127.0.0.1', 2, "Adios"),
    io:format("TEST: Esperando a mayoria instancia 2~n",[]),
    esperar_mayoria(S, 2),
    io:format("TEST: Hay instancia 2 para mayoria~n",[]),
    N_decid = num_decididos(S, 2),
    if N_decid =/= (NumServidores - 2) -> exit("Algun sordo sabe decision!!");
       true -> ok
    end,
     
    paxos:escucha('n1@127.0.0.1'),
    paxos:start_instancia('n1@127.0.0.1', 2, "WWW"),
     io:format("TEST: Esperando a N-1 nodo(solo uno sordo) instancia 2~n",[]),
    esperar_n_nodos(S, 2, NumServidores - 1),
     io:format("TEST: Hay instancia 2 N-1 nodo(solo uno sordo)~n",[]),

    N2_decid = num_decididos(S, 2),
    if N2_decid =/= (NumServidores -1) -> exit("Algun sordo sabe decision!!");
       true -> ok
    end,
    
    paxos:escucha('n5@127.0.0.1'),
    timer:sleep(50),
    paxos:start_instancia('n5@127.0.0.1', 2, "ZZZ"),
    timer:sleep(20),
     io:format("TEST: Esperando a todos los nodos instancia 2~n",[]),
    esperar_n_nodos(S, 2, NumServidores),
     io:format("TEST: Hay instancia 2 para todos~n",[]),
    % parar VMs Erlang
    lists:foreach(fun(X) -> paxos:stop(X) end, S ),
                  
    ?debugFmt(".... Superado~n",[]).  


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
olvidando() ->
    ?debugFmt("Test: Olvidando..~n",[]),
    
   % Crear 6 nodos Paxos
    NumServidores = 6,
    L_N = lists:seq(1,NumServidores),
    S = servidores(L_N),
    lists:foreach(fun(X) -> paxos:start(S,?HOST,lists:concat([n,X])) end, L_N),
    
    timer:sleep(?T_ESPERA),
    
    ?debugFmt("Comprobar min() inicial ---------~n",[]),
    lists:foreach(fun(NodoPaxos) ->
                        M = paxos:min(NodoPaxos),
			io:format("Minimo: ~p, nodo: ~p", [M, NodoPaxos]),
                        if M > 1 -> exit("min() erroneo 1 !!");
                           true -> ok
                        end
                  end,
                  S),
     %% Poner en marcha varios acuerdos
    paxos:start_instancia('n1@127.0.0.1', 1, "11"),
    paxos:start_instancia('n2@127.0.0.1', 2, "22"),
    paxos:start_instancia('n3@127.0.0.1', 3, "33"),
    paxos:start_instancia('n1@127.0.0.1', 7, "77"),
    paxos:start_instancia('n2@127.0.0.1', 8, "88"),

    esperar_n_nodos(S, 2, NumServidores),
    
    %%% Es correcto min() ??
    lists:foreach(fun(X) ->
                      M = paxos:min(X),
		      io:format("Minimo: ~p, nodo: ~p", [M, S]),
                      if M /= 1 -> exit("min() erroneo 2 !!");
                         true -> ok
                      end
                  end,
                  S),
                  
 
    %% Hechos Instancias 1 y 2 para todos  -> Cambia min() ?
    lists:foreach(fun(N) -> paxos:hecho(N, 1) end, S),
    lists:foreach(fun(N) -> paxos:hecho(N, 2) end, S),
    lists:foreach(fun({X, N}) -> paxos:start_instancia(X, 8 + N, "xx") end, 
                lists:zip(S, L_N) ),
    timer:sleep(12),
    L_Min = [ paxos:min(X) || X <-  S ],
    All_3 = lists:foldl(fun(X,Previo) -> (X == 3) and Previo end, true, L_Min),
    if not All_3 -> exit("min() no ha avanzado despues de hecho()");
       true -> ok
    end,
    
    % parar VMs Erlang
    lists:foreach(fun(X) -> paxos:stop(X) end, S ),

    ?debugFmt(".... Superado~n",[]).  


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
muchas_instancias() ->
    ?debugFmt("Test: Muchas instancias..~n",[]),
    
   % Crear 3 nodos Paxos
    NumServ = 3,
    L_N = lists:seq(1,NumServ),
    S = servidores(L_N),
    lists:foreach(fun(X) -> paxos:start(S,?HOST,lists:concat([n,X])) end, L_N),
    
    
    timer:sleep(?T_ESPERA),
        
     % Ejecutar 30 instancias, en lotes de 3 instancias a la vez
    % Es decir, 10 lotes
    lists:foreach(fun(Lote) ->
                    lists:foreach(fun (I) ->
                                if I >= 6 -> esperar_n_nodos(S, I - 3, NumServ);
                                   true -> ok
                                end,
                                                          
                                lists:foreach(fun({X, P_S}) -> 
                                                paxos:start_instancia(X, I, 
                                                              (I * 10) + P_S )
                                              end,
                                              lists:zip(S, L_N) )
                        end,
                        lists:seq( ((Lote-1) * 3) + 1, Lote *3) )
                  end,
                  lists:seq(1,10) ),
    
   %% Solo falta esperar a que termine la decisión de las últimas 3 instancias
    lists:foreach(fun(Inst) ->
                        esperar_n_nodos(S, Inst, NumServ) 
                  end,
                  lists:seq(28,30) ),
    
    % parar VMs Erlang
    lists:foreach(fun(X) -> paxos:stop(X) end, S ),

    ?debugFmt(".... Superado~n",[]).  


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Muchas instancias paxos con comunicación no fiable
muchas_instancias_no_fiable() ->
    ?debugFmt("Test: Muchas instancias comunicacion no fiable..~n",[]),
    
   % Crear 3 nodos Paxos
    NumServ = 3,
    L_N = lists:seq(1,NumServ),
    S = servidores(L_N),
    lists:foreach(fun(X) -> paxos:start(S,?HOST,lists:concat([n,X])) end, L_N),
    
    
    timer:sleep(?T_ESPERA),
        
    %% marcar comunicación no fiable
    lists:foreach(fun(X) -> paxos:comm_no_fiable(X) end, S),

     % Ejecutar 30 instancias, en lotes de 3 instancias a la vez
    % Es decir, 10 lotes
    lists:foreach(fun(Lote) ->
                    lists:foreach(fun (I) ->
                                if I >= 6 -> esperar_n_nodos(S, I - 3, NumServ);
                                   true -> ok
                                end,
                                                           
                                lists:foreach(fun({X, P_S}) -> 
                                                paxos:start_instancia(X, I, 
                                                              (I * 10) + P_S )
                                              end,
                                              lists:zip(S, L_N) )
                        end,
                        lists:seq( ((Lote-1) * 3) + 1, Lote *3) )
                  end,
                  lists:seq(1,10) ),
     
   %% Solo falta esperar a que termine la decisión de las últimas 3 instancias
    lists:foreach(fun(Inst) ->
                        esperar_n_nodos(S, Inst, NumServ) 
                  end,
                  lists:seq(28,30) ),
    
     % parar VMs Erlang
    lists:foreach(fun(X) -> paxos:stop(X) end, S ),

    ?debugFmt(".... Superado~n",[]).  


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Definir particiones de servidore Erlang
particionar(Lista_particiones) ->
    lists:foreach(fun(P) ->
                    lists:foreach(fun(X) -> paxos:limitar_acceso(X, P),
		io:format("limitar: X=~p, P:~p~n",[X,P])
		 end, P)
                  end,
                  Lista_particiones),
                  
    timer:sleep(50).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% No hay ninguna decision en particionado sin ninguna mayoría
no_hay_decision_si_particionado() ->
    ?debugFmt("Test: No hay decision si red particionada sin mayoria..~n",[]),
    
   % Crear 5 nodos Paxos
    NumServ = 5,
    L_N = lists:seq(1,NumServ),
    S = servidores(L_N),
    lists:foreach(fun(X) -> paxos:start(S,?HOST,lists:concat([n,X])),
	particionar( [ servidores([1,2]), servidores([3]), servidores([4,5]) ] ) end, L_N),
    
    timer:sleep(?T_ESPERA),


    paxos:start_instancia('n2@127.0.0.1', 1, "11"),

    %% no hay ninguna mayoría luego no puede haber ningun valor
    comprobar_max(S, 1, 0),
          paxos:start_instancia('n5@127.0.0.1', 1, "11"),
    comprobar_max(S, 1, 0),
     % parar VMs Erlang
    lists:foreach(fun(X) -> paxos:stop(X) end, S ),
    
    ?debugFmt(".... Superado~n",[]).  
    

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% No hay ninguna decision en particionado sin ninguna mayoría
no_hay_decision_si_particionado2() ->
    ?debugFmt("Test: No hay decision si red particionada sin mayoria..~n",[]),
    
   % Crear 5 nodos Paxos
    NumServ = 5,
    L_N = lists:seq(1,NumServ),
    S = servidores(L_N),
    lists:foreach(fun(X) -> paxos:start(S,?HOST,lists:concat([n,X]),
	particionar( [ servidores([1,2,3]), servidores([3,4,5]) ] )) end, L_N),
    
    timer:sleep(?T_ESPERA),

       paxos:start_instancia('n2@127.0.0.1', 1, "11"),
    
    %% no hay ninguna mayoría luego no puede haber ningun valor
    comprobar_max(S, 1, 0),
  
     % parar VMs Erlang
    lists:foreach(fun(X) -> paxos:stop(X) end, S ),
    
    ?debugFmt(".... Superado~n",[]).  
    
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 
decision_en_particion_mayoritaria() ->
    ?debugFmt("Test: Hay decision en la particion mayoritaria..~n",[]),
    
   % Crear 5 nodos Paxos
    NumServ = 5,
    L_N = lists:seq(1,NumServ),
    S = servidores(L_N),
    lists:foreach(fun(X) -> paxos:start(S,?HOST,lists:concat([n,X])) end, L_N),
    
    timer:sleep(?T_ESPERA),
    
    particionar( [ servidores([1]), servidores([2, 3, 4]), servidores([5]) ] ),

    paxos:start_instancia('n2@127.0.0.1', 1, "11"),
    
    %% Hay una mayoría
    esperar_mayoria(S, 1),
    
     % parar VMs Erlang
    lists:foreach(fun(X) -> paxos:stop(X) end, S ),
    
    ?debugFmt(".... Superado~n",[]).  
    
    
%%%%%%%%%%%%%%%%% GENERADORES DE TEST %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 
basico_test_() ->
    {inorder,
            [{spawn, {timeout, 1, ?_test(unico_proponente())}},
            {spawn, {timeout, 1, ?_test(varios_propo_un_valor())}},
            {spawn, {timeout, 2, ?_test(varios_propo_varios_valores())}},
            {spawn, {timeout, 8, ?_test(instancias_fuera_orden())}},
            {spawn, {timeout, 6, ?_test(proponentes_sordos())}}      
                   
            ]}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 
adicionales_test_() ->
    {inorder,
            [ {spawn, {timeout, 4, ?_test(olvidando())}},
              {spawn, {timeout, 6, ?_test(muchas_instancias())}},
              {spawn, {timeout, 10, ?_test(muchas_instancias_no_fiable())}}
            ]}.
      
      
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
particiones_test_() ->
    {inorder,
            [
             {spawn, {timeout, 8, ?_test(no_hay_decision_si_particionado())}},
             {spawn, {timeout, 8, ?_test(decision_en_particion_mayoritaria())}}
            ]}.

