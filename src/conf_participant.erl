%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2013-2025, 2600Hz
%%% @doc Conference participant process
%%% @author Karl Anderson
%%% @author James Aimonetti
%%%
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(conf_participant).
-behaviour(gen_listener).

%% API
-export([start_link/1]).
-export([relay_amqp/2]).
-export([handle_conference_error/2]).

-export([consume_call_events/1]).
-export([conference/1, set_conference/2]).
-export([discovery_event/1, set_discovery_event/2]).
-export([call/1]).

-export([join_local/1, join_remote/2]).

-export([set_name_pronounced/2]).

-export([mute/1, unmute/1, toggle_mute/1]).
-export([deaf/1, undeaf/1, toggle_deaf/1]).
-export([hangup/1]).
-export([state/1]).

-export([handle_conference_event/2]).

%% gen_server callbacks
-export([init/1
        ,handle_call/3
        ,handle_cast/2
        ,handle_info/2
        ,handle_event/2
        ,terminate/2
        ,code_change/3
        ]).

-include("conference.hrl").

-define(SERVER, ?MODULE).

-define(RESPONDERS, [{{?MODULE, 'relay_amqp'}
                     ,[{<<"call_event">>, <<"*">>}]
                     },
                     {{?MODULE, 'handle_conference_event'}
                     ,[{<<"conference">>, <<"event">>}]
                     }
                    ,{{?MODULE, 'handle_conference_error'}
                     ,[{<<"conference">>, <<"error">>}]
                     }
                    ]).
-define(QUEUE_NAME, <<>>).
-define(QUEUE_OPTIONS, []).
-define(CONSUME_OPTIONS, []).

-record(participant, {participant_id = 0 :: non_neg_integer()
                     ,call :: kapps_call:call() | 'undefined'
                     ,moderator = 'false' :: boolean()
                     ,muted = 'false' :: boolean()
                     ,deaf = 'false' :: boolean()
                     ,call_event_consumers = [] :: kz_term:pids()
                     ,in_conference = 'false' :: boolean()
                     ,conference :: kapps_conference:conference() | 'undefined'
                     ,discovery_event = kz_json:new() :: kz_json:object()
                     ,remote = 'false' :: boolean()
                     ,name_pronounced :: conf_pronounced_name:name_pronounced()
                     }).
-type participant() :: #participant{}.

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Starts the server.
%% @end
%%------------------------------------------------------------------------------
-spec start_link(kapps_call:call()) -> kz_types:startlink_ret().
start_link(Call) ->
    CallId = kapps_call:call_id(Call),
    Bindings = [{'call', [{'callid', CallId}]}
               ,{'self', []}
               ],
    gen_listener:start_link(?SERVER, [{'responders', ?RESPONDERS}
                                     ,{'bindings', Bindings}
                                     ,{'queue_name', ?QUEUE_NAME}
                                     ,{'queue_options', ?QUEUE_OPTIONS}
                                     ,{'consume_options', ?CONSUME_OPTIONS}
                                     ], [Call]).

-spec conference(pid()) -> {'ok', kapps_conference:conference()}.
conference(Srv) -> gen_listener:call(Srv, {'get_conference'}).

-spec set_conference(kapps_conference:conference(), pid()) -> 'ok'.
set_conference(Conference, Srv) -> gen_listener:cast(Srv, {'set_conference', Conference}).

-spec discovery_event(pid()) -> {'ok', kz_json:object()}.
discovery_event(Srv) -> gen_listener:call(Srv, {'get_discovery_event'}).

-spec set_discovery_event(kz_json:object(), pid()) -> 'ok'.
set_discovery_event(DE, Srv) -> gen_listener:cast(Srv, {'set_discovery_event', DE}).

-spec set_name_pronounced(conf_pronounced_name:name_pronounced(), pid()) -> 'ok'.
set_name_pronounced(Name, Srv) -> gen_listener:cast(Srv, {'set_name_pronounced', Name}).

-spec call(pid()) -> {'ok', kapps_call:call()}.
call(Srv) -> gen_listener:call(Srv, {'get_call'}).

-spec join_local(pid()) -> 'ok'.
join_local(Srv) -> gen_listener:cast(Srv, 'join_local').

-spec join_remote(pid(), kz_json:object()) -> 'ok'.
join_remote(Srv, JObj) -> gen_listener:cast(Srv, {'join_remote', JObj}).

-spec state(pid()) -> 'ok'.
state(Srv) -> gen_listener:call(Srv, {'state'}).

-spec mute(pid()) -> 'ok'.
mute(Srv) -> gen_listener:cast(Srv, 'mute').

-spec unmute(pid()) -> 'ok'.
unmute(Srv) -> gen_listener:cast(Srv, 'unmute').

-spec toggle_mute(pid()) -> 'ok'.
toggle_mute(Srv) -> gen_listener:cast(Srv, 'toggle_mute').

-spec deaf(pid()) -> 'ok'.
deaf(Srv) -> gen_listener:cast(Srv, 'deaf').

-spec undeaf(pid()) -> 'ok'.
undeaf(Srv) -> gen_listener:cast(Srv, 'undeaf').

-spec toggle_deaf(pid()) -> 'ok'.
toggle_deaf(Srv) -> gen_listener:cast(Srv, 'toggle_deaf').

-spec hangup(pid()) -> 'ok'.
hangup(Srv) -> gen_listener:cast(Srv, 'hangup').

-spec consume_call_events(pid()) -> 'ok'.
consume_call_events(Srv) -> gen_listener:cast(Srv, {'add_consumer', self()}).

-spec relay_amqp(kz_json:object(), kz_term:proplist()) -> any().
relay_amqp(JObj, Props) ->
    _ = [kapps_call_command:relay_event(Pid, JObj)
         || Pid <- props:get_value('call_event_consumers', Props, []),
            is_pid(Pid)
        ],
    Srv = props:get_value('server', Props),
    kapps_call_command:relay_event(Srv, JObj).

-spec handle_conference_error(kz_json:object(), kz_term:proplist()) -> 'ok'.
handle_conference_error(JObj, Props) ->
    'true' = kapi_conference:conference_error_v(JObj),
    lager:debug("conference error: ~p", [JObj]),
    case kz_json:get_value([<<"Request">>, <<"Application-Name">>], JObj) of
        <<"participants">> ->
            Srv = props:get_value('server', Props),
            gen_listener:cast(Srv, {'sync_participant', []});
        _Else -> 'ok'
    end.

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Initializes the server.
%% @end
%%------------------------------------------------------------------------------
-spec init([kapps_call:call()]) -> {'ok', participant()}.
init([Call]) ->
    process_flag('trap_exit', 'true'),
    _ = kapps_call:put_callid(Call),
    _ = start_sanity_check_timer(),
    {'ok', #participant{call=Call}}.

-spec start_sanity_check_timer() -> reference().
start_sanity_check_timer() ->
    start_sanity_check_timer(kapps_config:get_integer(?CONFIG_CAT, <<"participant_sanity_check_ms">>, ?MILLISECONDS_IN_MINUTE)).

-spec start_sanity_check_timer(pos_integer()) -> reference().
start_sanity_check_timer(Timeout) ->
    erlang:send_after(Timeout, self(), 'sanity_check').

%%------------------------------------------------------------------------------
%% @doc Handling call messages.
%% @end
%%------------------------------------------------------------------------------
-spec handle_call(any(), kz_term:pid_ref(), participant()) -> kz_types:handle_call_ret_state(participant()).
handle_call({'get_conference'}, _, #participant{conference='undefined'}=P) ->
    {'reply', {'error', 'not_provided'}, P};
handle_call({'get_conference'}, _, #participant{conference=Conf}=P) ->
    {'reply', {'ok', Conf}, P};
handle_call({'get_discovery_event'}, _, #participant{discovery_event=DE}=P) ->
    {'reply', {'ok', DE}, P};
handle_call({'get_call'}, _, #participant{call=Call}=P) ->
    {'reply', {'ok', Call}, P};
handle_call({'state'}, _, Participant) ->
    {'reply', Participant, Participant};
handle_call(_Request, _, P) ->
    {'reply', {'error', 'unimplemented'}, P}.

%%------------------------------------------------------------------------------
%% @doc Handling cast messages.
%% @end
%%------------------------------------------------------------------------------
-spec handle_cast(any(), participant()) -> kz_types:handle_cast_ret_state(participant()).
handle_cast({'execute_complete', <<"conference">>}, #participant{call = Call} = Participant) ->
    kapps_call_command:hangup(Call),
    {'stop', {'shutdown', 'complete'}, Participant};
handle_cast({'execute_complete', <<"bridge">>}, #participant{call = Call} = Participant) ->
    kapps_call_command:hangup(Call),
    {'stop', {'shutdown', 'complete'}, Participant};
handle_cast('hungup', #participant{call=Call}=Participant) ->
    kapps_call_command:hangup(Call),
    {'stop', {'shutdown', 'hungup'}, Participant};
handle_cast('pivoted', Participant) ->
    {'stop', 'normal', Participant};
handle_cast({'channel_replaced', NewCallId}
           ,#participant{call=Call}=Participant
           ) ->
    kz_log:put_callid(NewCallId),
    NewCall = kapps_call:set_call_id(NewCallId, Call),
    lager:info("updated call to use ~s instead", [NewCallId]),
    gen_listener:add_binding(self(), 'call', [{'callid', NewCallId}]),
    {'noreply', Participant#participant{call=NewCall}};

handle_cast({'gen_listener', {'created_queue', Q}}
           ,#participant{conference='undefined'
                        ,call=Call
                        }=P) ->
    lager:debug("participant queue created ~s", [Q]),
    {'noreply', P#participant{call=kapps_call:set_controller_queue(Q, Call)}};
handle_cast({'gen_listener', {'created_queue', Q}}, #participant{conference=Conference
                                                                ,call=Call
                                                                }=P) ->
    lager:debug("participant queue created with conference set : ~s", [Q]),
    {'noreply', P#participant{call=kapps_call:set_controller_queue(Q, Call)
                             ,conference=kapps_conference:set_controller_queue(Q, Conference)
                             }};
handle_cast('hangup', Participant) ->
    lager:debug("received in-conference command, hangup participant"),
    gen_listener:cast(self(), 'hungup'),
    {'noreply', Participant};
handle_cast({'add_consumer', C}, #participant{call_event_consumers=Cs}=P) ->
    lager:debug("adding call event consumer ~p", [C]),
    link(C),
    {'noreply', P#participant{call_event_consumers=[C|Cs]}};
handle_cast({'remove_consumer', C}, #participant{call_event_consumers=Cs}=P) ->
    lager:debug("removing call event consumer ~p", [C]),
    {'noreply', P#participant{call_event_consumers=[C1 || C1 <- Cs, C=/=C1]}};
handle_cast({'set_conference', Conference}, Participant=#participant{call=Call}) ->
    ConferenceId = kapps_conference:id(Conference),
    CallId = kapps_call:call_id(Call),
    lager:debug("received conference data for conference ~s", [ConferenceId]),
    gen_listener:add_binding(self(), 'conference', [{'restrict_to', [{'event', {ConferenceId, CallId}}]}]),
    {'noreply', Participant#participant{conference=Conference}};
handle_cast({'set_discovery_event', DE}, #participant{}=Participant) ->
    {'noreply', Participant#participant{discovery_event=DE}};
handle_cast({'set_name_pronounced', Name}, #participant{}=Participant) ->
    _ = set_enter_exit_sounds(Name, Participant),
    {'noreply', Participant#participant{name_pronounced = Name}};
handle_cast({'gen_listener',{'is_consuming','true'}}, Participant) ->
    lager:debug("now consuming messages"),
    {'noreply', Participant};
handle_cast(_Message, #participant{conference='undefined'}=Participant) ->
    %% ALL MESSAGES BELOW THIS ARE CONSUMED HERE UNTIL THE CONFERENCE IS KNOWN
    lager:debug("ignoring message prior to conference discovery: ~p"
               ,[_Message]
               ),
    {'noreply', Participant};
handle_cast('join_local', #participant{call=Call
                                      ,conference=Conference
                                      }=Participant) ->
    lager:debug("sending command for participant to join local conference ~s", [kapps_conference:id(Conference)]),
    send_conference_command(Conference, Call),
    {'noreply', Participant};

handle_cast({'join_remote', JObj}, #participant{call=Call
                                               ,conference=Conference
                                               ,name_pronounced=Name
                                               }=Participant) ->
    lager:debug("sending command for participant to join remote conference ~s", [kapps_conference:id(Conference)]),
    Route = binary:replace(kz_json:get_value(<<"Switch-URL">>, JObj)
                          ,<<"mod_sofia">>
                          ,kapps_conference:inter_conference_extension()
                          ),
    _ = maybe_set_interaction_id(Call, JObj),
    bridge_to_conference(Route, Conference, Call, Name),
    {'noreply', Participant#participant{remote='true'}};
handle_cast({'sync_participant', JObj}, #participant{call=Call}=Participant) ->
    Event = kz_json:get_ne_binary_value(<<"Event">>, JObj),
    {'noreply', sync_participant(Event, JObj, Call, Participant)};
handle_cast(_Cast, Participant) ->
    lager:debug("unhandled cast: ~p", [_Cast]),
    {'noreply', Participant}.

%%------------------------------------------------------------------------------
%% @doc Handling all non call/cast messages.
%% @end
%%------------------------------------------------------------------------------
-spec handle_info(any(), participant()) -> kz_types:handle_info_ret_state(participant()).
handle_info({'EXIT', Consumer, _R}, #participant{call_event_consumers=Consumers}=P) ->
    lager:debug("call event consumer ~p died: ~p", [Consumer, _R]),
    Cs = [C || C <- Consumers, C =/= Consumer],
    {'noreply', P#participant{call_event_consumers=Cs}, 'hibernate'};
handle_info('sanity_check', #participant{call=Call}=State) ->
    _ = case kapps_call_events:is_destroyed(Call) of
            'false' -> start_sanity_check_timer();
            'true' ->
                lager:info("channel not found, going down"),
                gen_listener:cast(self(), 'hungup')
        end,
    {'noreply', State};
handle_info({'hungup', CallId}, #participant{call=Call}=Participant) ->
    case kapps_call:call_id(Call) of
        CallId ->
            lager:debug("recv hungup for matching call id ~s", [CallId]),
            {'stop', {'shutdown', 'hungup'}, Participant};
        _NewCallId ->
            lager:debug("recv hungup for old call id ~s, ignoring", [CallId]),
            {'noreply', Participant}
    end;
handle_info({'amqp_msg', JObj}, Participant) ->
    handle_amqp_msg(JObj, kz_api:event_type(JObj)),
    {'noreply', Participant};
handle_info(_Msg, Participant) ->
    lager:debug_unsafe("unhandled message ~p", [_Msg]),
    {'noreply', Participant}.

handle_amqp_msg(JObj, {<<"call_event">>, <<"CHANNEL_EXECUTE_COMPLETE">>}) ->
    gen_listener:cast(self(), {'execute_complete', kz_call_event:application_name(JObj)});
handle_amqp_msg(_JObj, _Type) -> 'ok'.

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec handle_event(kz_json:object(), participant()) -> gen_listener:handle_event_return().
handle_event(JObj, #participant{call_event_consumers=Consumers
                               ,call=Call
                               }) ->
    CallId = kapps_call:call_id(Call),
    case {kz_api:event_type(JObj)
         ,kz_call_event:call_id(JObj)
         }
    of
        {{<<"call_event">>, <<"CHANNEL_DESTROY">>}, CallId} ->
            lager:debug("received channel hangup event, maybe terminating"),
            _Ref = erlang:send_after(3 * ?MILLISECONDS_IN_SECOND, self(), {'hungup', CallId}),
            'ok';
        {{<<"call_event">>, <<"CHANNEL_PIVOT">>}, CallId} ->
            handle_channel_pivot(JObj, Call);
        {{<<"call_event">>, <<"CHANNEL_REPLACED">>}, CallId} ->
            handle_channel_replaced(JObj, self());
        {_, _} -> 'ok'
    end,
    {'reply', [{'call_event_consumers', Consumers}
              ,{'is_participant', 'true'}
              ]}.

-spec handle_channel_replaced(kz_json:object(), kz_types:server_ref()) -> 'ok'.
handle_channel_replaced(JObj, Srv) ->
    NewCallId = kz_call_event:replaced_by(JObj),
    lager:info("channel has been replaced with ~s", [NewCallId]),
    gen_listener:cast(Srv, {'channel_replaced', NewCallId}).

-spec handle_channel_pivot(kz_json:object(), kapps_call:call()) -> 'ok'.
handle_channel_pivot(JObj, Call) ->
    case kz_json:get_ne_binary_value(<<"Application-Data">>, JObj) of
        'undefined' -> lager:info("no app data to pivot");
        FlowBin ->
            lager:info("recv channel pivot with flow ~s", [FlowBin]),
            unbridge_from_conference(Call),

            Req = [{<<"Flow">>, kz_json:decode(FlowBin)}
                  ,{<<"Call">>, kapps_call:to_json(Call)}
                  | kz_api:default_headers(?APP_NAME, ?APP_VERSION)
                  ],
            _ = kz_amqp_worker:cast(Req, fun kapi_callflow:publish_resume/1),
            lager:info("stopping the conf participant"),
            gen_listener:cast(self(), 'pivoted')
    end.

%%------------------------------------------------------------------------------
%% @doc This function is called by a `gen_server' when it is about to
%% terminate. It should be the opposite of `Module:init/1' and do any
%% necessary cleaning up. When it returns, the `gen_server' terminates
%% with Reason. The return value is ignored.
%%
%% @end
%%------------------------------------------------------------------------------
-spec terminate(any(), participant()) -> 'ok'.
terminate(_Reason, #participant{name_pronounced = Name}) ->
    maybe_clear(Name),
    lager:debug("conference participant execution has been stopped: ~p", [_Reason]).

-spec maybe_clear(conf_pronounced_name:name_pronounced()) -> 'ok'.
maybe_clear({'temp_doc_id', AccountId, MediaId}) ->
    AccountDb = kzs_util:format_account_db(AccountId),
    lager:debug("deleting doc: ~s/~s", [AccountDb, MediaId]),
    _ = kz_datamgr:del_doc(AccountDb, MediaId),
    'ok';
maybe_clear(_) -> 'ok'.

%%------------------------------------------------------------------------------
%% @doc Convert process state when code is changed.
%% @end
%%------------------------------------------------------------------------------
-spec code_change(any(), participant(), any()) -> {'ok', participant()}.
code_change(_OldVsn, Participant, _Extra) ->
    {'ok', Participant}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec log_conference_join(boolean(), non_neg_integer(), kapps_conference:conference()) -> 'ok'.
log_conference_join('true'=_Moderator, ParticipantId, Conference) ->
    lager:debug("caller has joined the local conference ~s as moderator ~p", [kapps_conference:name(Conference), ParticipantId]);
log_conference_join('false'=_Moderator, ParticipantId, Conference) ->
    lager:debug("caller has joined the local conference ~s as member ~p", [kapps_conference:name(Conference), ParticipantId]).

-spec sync_participant(kz_term:ne_binary(), kz_json:objects(), kapps_call:call(), participant()) -> participant().
sync_participant(<<"add-member">>, JObj, Call, Participant) ->
    sync_participant(JObj, Call, Participant);
sync_participant(_Event, _JObj, _Call, Participant) ->
    Participant.

-spec sync_participant(kz_json:objects(), kapps_call:call(), participant()) ->
          participant().
sync_participant(JObj, Call, #participant{in_conference='false'
                                         ,conference=Conference
                                         ,discovery_event=DiscoveryEvent
                                         }=Participant) ->
    ParticipantId = kz_json:get_value(<<"Participant-ID">>, JObj),
    IsModerator = kz_json:is_true([<<"Conference-Channel-Vars">>, <<"Is-Moderator">>], JObj),
    log_conference_join(IsModerator, ParticipantId, Conference),
    _ = kz_process:spawn(fun notify_requestor/4, [kapps_call:controller_queue(Call)
                                                 ,ParticipantId
                                                 ,DiscoveryEvent
                                                 ,kapps_conference:id(Conference)
                                                 ]),
    Muted = kz_json:is_false([<<"Conference-Channel-Vars">>, <<"Speak">>], JObj),
    Deaf = kz_json:is_false([<<"Conference-Channel-Vars">>, <<"Hear">>], JObj),
    Participant#participant{in_conference='true'
                           ,participant_id=ParticipantId
                           ,muted=Muted
                           ,deaf=Deaf
                           ,moderator=IsModerator
                           };
sync_participant(JObj, _Call, #participant{in_conference='true'}=Participant) ->
    lager:debug("caller has is still in the conference"),
    Muted = kz_json:is_false([<<"Conference-Channel-Vars">>, <<"Speak">>], JObj),
    Deaf = kz_json:is_false([<<"Conference-Channel-Vars">>, <<"Hear">>], JObj),
    Participant#participant{in_conference='true'
                           ,muted=Muted
                           ,deaf=Deaf
                           }.

-spec notify_requestor(kz_term:ne_binary(), non_neg_integer(), kz_json:object(), kz_term:ne_binary()) -> 'ok'.
notify_requestor(MyQ, MyId, DiscoveryEvent, ConferenceId) ->
    case kz_api:server_id(DiscoveryEvent) of
        'undefined' -> 'ok';
        <<>> -> 'ok';
        RequestorQ ->
            Resp = [{<<"Conference-ID">>, ConferenceId}
                   ,{<<"Participant-ID">>, MyId}
                   | kz_api:default_headers(MyQ, ?APP_NAME, ?APP_VERSION)
                   ],
            Publisher = fun(P) -> kapi_conference:publish_discovery_resp(RequestorQ, P) end,
            kz_amqp_worker:cast(Resp, Publisher)
    end.

-spec should_process_interaction() -> boolean().
should_process_interaction() ->
    kapps_config:get_boolean(?INTERACTION_CAT, <<"process_conference">>, 'true').

-spec maybe_set_interaction_id(kapps_call:call(), kz_json:object()) -> any().
maybe_set_interaction_id(Call, JObj) ->
    InteractionId = kz_json:get_ne_binary_value(<<?CALL_INTERACTION_ID>>, JObj),
    maybe_set_interaction_id(Call, InteractionId, should_process_interaction()).

maybe_set_interaction_id(_Call, _InteractionId, 'false') -> 'ok';
maybe_set_interaction_id(_Call, 'undefined', 'true') -> 'ok';
maybe_set_interaction_id(Call, InteractionId, 'true') ->
    ChannelVars = kz_json:from_list([{<<?CALL_INTERACTION_ID>>, InteractionId}]),
    kapps_call_command:set(ChannelVars, kz_json:new(), Call).

-spec bridge_to_conference(kz_term:ne_binary(), kapps_conference:conference(), kapps_call:call(), conf_pronounced_name:name_pronounced()) -> 'ok'.
bridge_to_conference(Route, Conference, Call, Name) ->
    lager:debug("bridging to conference running at '~s'", [Route]),

    AccountRealm = get_account_realm(Call),
    ConferenceId = kapps_conference:id(Conference),

    ConferenceURI = list_to_binary(["sip:", ConferenceId, "@", AccountRealm]),
    Endpoint = kz_json:from_list([{<<"Invite-Format">>, <<"route">>}
                                 ,{<<"Route">>, Route}
                                 ,{<<"Outbound-Caller-ID-Number">>, kapps_call:caller_id_number(Call)}
                                 ,{<<"Outbound-Caller-ID-Name">>, kapps_call:caller_id_name(Call)}
                                 ,{<<"Ignore-Early-Media">>, <<"true">>}
                                 ,{<<"To-URI">>, ConferenceURI}
                                 ,{<<"Custom-Channel-Vars">>, remote_ccvs(remove_billing_ccvs(Call))}
                                 ,{<<"Custom-SIP-Headers">>, conference_headers(Conference, Call, Name)}
                                 ,{<<"Bypass-Proxy">>, 'true'}
                                 ]),
    Command = [{<<"Application-Name">>, <<"bridge">>}
              ,{<<"Endpoints">>, [Endpoint]}
              ,{<<"Timeout">>, 20}
              ,{<<"Dial-Endpoint-Method">>, <<"single">>}
              ,{<<"Ignore-Early-Media">>, <<"false">>}
              ,{<<"Hold-Media">>, <<"silence">>}
              ],
    kapps_call_command:send_command(Command, Call).

-spec conference_headers(kapps_conference:conference(), kapps_call:call(), conf_pronounced_name:name_pronounced()) ->
          kz_json:object().
conference_headers(Conference, Call, Name) ->
    IsModerator = kapps_conference:moderator(Conference),
    ConferenceId = kapps_conference:id(Conference),
    DiscoveryJObj = kapps_conference:discovery_request(Conference),

    ConfSIPHeaders = [{<<"X-Conference-Moderator">>, IsModerator}
                     ,{<<"X-Conference-Account-ID">>, kapps_call:account_id(Call)}
                     ,{<<"X-Conference-ID">>, ConferenceId}
                     ,{<<"X-Conference-Play-Welcome">>, kz_json:get_boolean_value(<<"Play-Welcome">>, DiscoveryJObj)}
                     ,{<<"X-Conference-Play-Welcome-Media">>, kz_json:get_ne_binary_value(<<"Play-Welcome-Media">>, DiscoveryJObj)}
                     ,{<<"X-Conference-Play-Entry-Tone">>, kz_json:get_boolean_value(<<"Play-Entry-Tone">>, DiscoveryJObj)}
                     ,{<<"X-Conference-Play-Exit-Tone">>, kz_json:get_boolean_value(<<"Play-Exit-Tone">>, DiscoveryJObj)}
                     ,{<<"X-Conference-End-On-Leave">>, kz_json:get_boolean_value(<<"End-On-Leave">>, DiscoveryJObj)}
                     ,{<<"X-Conference-End-On-Last-Member-Leave">>, kz_json:get_boolean_value(<<"End-On-Last-Member-Leave">>, DiscoveryJObj)}
                     ,{<<"X-Conference-Participant-Join-Video-Muted">>, kz_json:get_boolean_value(<<"Participant-Join-Video-Muted">>, DiscoveryJObj)}
                     | deaf_muted_headers(DiscoveryJObj, IsModerator) ++ name_pronounced_headers(Name)
                     ],
    kz_json:from_list(
      props:set_values(ConfSIPHeaders, kapps_call:remote_bridge_auth_headers(Call))
     ).

-spec deaf_muted_headers(kz_json:object(), kz_term:api_boolean()) -> kz_term:proplist().
deaf_muted_headers(DiscoveryJObj, 'true') ->
    [{<<"X-Conference-Moderator-Join-Deaf">>, kz_json:get_boolean_value(<<"Moderator-Join-Deaf">>, DiscoveryJObj)}
    ,{<<"X-Conference-Moderator-Join-Muted">>, kz_json:get_boolean_value(<<"Moderator-Join-Muted">>, DiscoveryJObj)}
    ];
deaf_muted_headers(DiscoveryJObj, _) ->
    [{<<"X-Conference-Member-Join-Deaf">>, kz_json:get_boolean_value(<<"Member-Join-Deaf">>, DiscoveryJObj)}
    ,{<<"X-Conference-Member-Join-Muted">>, kz_json:get_boolean_value(<<"Member-Join-Muted">>, DiscoveryJObj)}
    ].

%% @doc meant to be used only by `bridge_to_conference/4' function that way kazoo doesn't
%%      count inter-FS legs as outbound trunks (flat_rate).
%% @end
-spec remove_billing_ccvs(kapps_call:call()) -> kapps_call:call().
remove_billing_ccvs(Call) ->
    kapps_call:remove_custom_channel_vars([<<"Reseller-Billing">>, <<"Account-Billing">>], Call).

-spec remote_ccvs(kapps_call:call()) -> kz_json:object().
remote_ccvs(Call) ->
    remote_ccvs(Call, kapps_call:authorizing_id(Call), kapps_call:account_id(Call)).

-spec remote_ccvs(kapps_call:call(), kz_term:api_ne_binary(), kz_term:ne_binary()) ->
          kz_json:object().
remote_ccvs(Call, 'undefined', _AccountId) ->
    kapps_call:custom_channel_vars(Call);
remote_ccvs(Call, EndpointId, AccountId) ->
    remote_ccvs(Call, kz_directory_endpoint:profile(EndpointId, AccountId)).

-spec remote_ccvs(kapps_call:call(), {'ok', kz_json:object()} | {'error', any()}) ->
          kz_json:object().
remote_ccvs(Call, {'ok', Endpoint}) ->
    EndpointCCVs = kz_json:get_json_value(<<"Custom-Channel-Vars">>, Endpoint, kz_json:new()),
    CallCCVs = kapps_call:custom_channel_vars(Call),

    EPMinusInvite = kz_json:delete_keys([<<"SIP-Invite-Request-URI">>, <<"SIP-Invite-To-URI">>]
                                       ,EndpointCCVs
                                       ),

    kz_json:set_values(kz_json:to_proplist(CallCCVs), EPMinusInvite);
remote_ccvs(Call, _) ->
    kapps_call:custom_channel_vars(Call).

-spec unbridge_from_conference(kapps_call:call()) -> 'ok'.
unbridge_from_conference(Call) ->
    kapps_call_command:unbridge(Call, 'undefined').

-spec get_account_realm(kapps_call:call()) -> kz_term:ne_binary().
get_account_realm(Call) ->
    case kzd_accounts:fetch_realm(kapps_call:account_id(Call)) of
        'undefined' -> <<"unknown">>;
        Realm -> Realm
    end.

-spec name_pronounced_headers(conf_pronounced_name:name_pronounced()) -> kz_term:proplist().
name_pronounced_headers('undefined') -> [];
name_pronounced_headers({_, AccountId, MediaId}) ->
    [{<<"X-Conf-Values-Pronounced-Name-Account-ID">>, AccountId}
    ,{<<"X-Conf-Values-Pronounced-Name-Media-ID">>, MediaId}
    ].

-spec send_conference_command(kapps_conference:conference(), kapps_call:call()) -> 'ok'.
send_conference_command(Conference, Call) ->
    DiscoveryJObj = kapps_conference:discovery_request(Conference),
    Command = [{<<"Application-Name">>, <<"conference">>}
              ,{<<"Conference-ID">>, kapps_conference:id(Conference)}
              ,{<<"Profile">>, get_profile_name(Conference)}
              ,{<<"Reinvite">>, 'false'}
              ,{<<"Account-ID">>, kapps_call:account_id(Call)}
              ,{<<"Join-Options">>
               ,kz_json:from_list(
                  [{<<"Mute">>, is_muted(Conference)}
                  ,{<<"Deaf">>, is_deaf(Conference)}
                  ,{<<"Moderator">>, kapps_conference:moderator(Conference)}
                  ,{<<"Video-Mute">>, is_video_muted(Conference)}
                  ,{<<"End-On-Leave">>, kz_json:get_boolean_value(<<"End-On-Leave">>, DiscoveryJObj)}
                  ,{<<"End-On-Last-Member-Leave">>, kz_json:get_boolean_value(<<"End-On-Last-Member-Leave">>, DiscoveryJObj)}
                  ]
                 )
               }
              ],
    kapps_call_command:send_command(Command, Call).

-spec is_muted(kapps_conference:conference()) -> boolean().
is_muted(Conference) ->
    case kapps_conference:moderator(Conference) of
        'true' -> kapps_conference:moderator_join_muted(Conference);
        _ -> kapps_conference:member_join_muted(Conference)
    end.

-spec is_deaf(kapps_conference:conference()) -> boolean().
is_deaf(Conference) ->
    case kapps_conference:moderator(Conference) of
        'true' -> kapps_conference:moderator_join_deaf(Conference);
        _ -> kapps_conference:member_join_deaf(Conference)
    end.

-spec is_video_muted(kapps_conference:conference()) -> boolean().
is_video_muted(Conference) ->
    kapps_conference:participant_join_video_muted(Conference).

-spec handle_conference_event(kz_json:object(), kz_term:proplist()) -> 'ok'.
handle_conference_event(JObj, Props) ->
    Srv = props:get_value('server', Props),
    gen_listener:cast(Srv, {'sync_participant', JObj}).

set_enter_exit_sounds({_, AccountId, MediaId}, #participant{conference=Conference
                                                           ,call=Call
                                                           ,moderator=IsModerator
                                                           }) ->
    EntrySounds = [play_entry_tone(IsModerator, Conference)
                  ,kapps_prompt:get_prompt(AccountId, MediaId)
                  ,kapps_call:get_prompt(Call, <<"conf-has_joined">>)
                  ],

    ExitSounds = [play_exit_tone(IsModerator, Conference)
                 ,kapps_prompt:get_prompt(AccountId, MediaId)
                 ,kapps_call:get_prompt(Call, <<"conf-has_left">>)
                 ],

    Fun = fun('undefined') -> 'false';
             (_) -> 'true'
          end,

    Sounds = [{<<"Conference-Entry-Sound">>, lists:filter(Fun, EntrySounds)}
             ,{<<"Conference-Exit-Sound">>, lists:filter(Fun, ExitSounds)}
             ],
    kapps_call_command:media_macro(Sounds, Call).

-spec play_exit_tone(boolean(), kapps_conference:conference()) -> kz_term:api_binary().
play_exit_tone('false', Conference) ->
    play_exit_tone_media(?EXIT_TONE(kapps_conference:account_id(Conference)), Conference);
play_exit_tone('true', Conference) ->
    play_exit_tone_media(?MOD_EXIT_TONE(kapps_conference:account_id(Conference)), Conference).

-spec play_exit_tone_media(kz_term:ne_binary(), kapps_conference:conference()) -> kz_term:api_binary().
play_exit_tone_media(Tone, Conference) ->
    case kapps_conference:play_exit_tone(Conference) of
        'false' -> 'undefined';
        MediaId = ?NE_BINARY -> kapps_prompt:get_prompt(kapps_conference:account_id(Conference), MediaId);
        _Else -> Tone
    end.

-spec play_entry_tone(boolean(), kapps_conference:conference()) -> kz_term:api_binary().
play_entry_tone('false', Conference) ->
    play_entry_tone_media(?ENTRY_TONE(kapps_conference:account_id(Conference)), Conference);
play_entry_tone('true', Conference) ->
    play_entry_tone_media(?MOD_ENTRY_TONE(kapps_conference:account_id(Conference)), Conference).

-spec play_entry_tone_media(kz_term:ne_binary(), kapps_conference:conference()) -> kz_term:api_binary().
play_entry_tone_media(Tone, Conference) ->
    case kapps_conference:play_entry_tone(Conference) of
        'false' -> 'false';
        MediaId = ?NE_BINARY -> kapps_prompt:get_prompt(kapps_conference:account_id(Conference), MediaId);
        _Else -> Tone
    end.

-spec get_profile_name(kapps_conference:conference()) -> kz_term:ne_binary().
get_profile_name(Conference) ->
    Default = list_to_binary([kapps_conference:id(Conference)
                             ,"_"
                             ,kapps_conference:account_id(Conference)
                             ]),
    kapps_conference:profile_name(Conference, Default).
