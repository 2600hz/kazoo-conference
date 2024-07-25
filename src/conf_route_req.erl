%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2011-2024, 2600Hz
%%% @doc
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(conf_route_req).

-include("conference.hrl").

-export([handle_req/2]).

-define(DEFAULT_ROUTE_WIN_TIMEOUT, 3000).
-define(ROUTE_WIN_TIMEOUT_KEY, <<"route_win_timeout">>).
-define(ROUTE_WIN_TIMEOUT, kapps_config:get_integer(?CONFIG_CAT, ?ROUTE_WIN_TIMEOUT_KEY, ?DEFAULT_ROUTE_WIN_TIMEOUT)).

-spec handle_req(kz_json:object(), kz_term:proplist()) -> 'ok'.
handle_req(Request, _Props) ->
    'true' = kapi_route:req_v(Request),
    kz_log:put_callid(Request),
    Call = kapps_call:from_route_req(Request),
    case kapps_call:request_user(Call) of
        <<"conference">> ->
            maybe_set_conference_id(Request, Call);
        <<"conference+", ConferenceId/binary>> ->
            CCVs = kz_json:set_values([{<<"Conference-ID">>, ConferenceId}]
                                     ,kapps_call:custom_channel_vars(Call)
                                     ),
            maybe_get_conference_account_id(Request, Call, CCVs);
        _Else ->
            'ok'
    end.


-spec maybe_set_conference_id(kz_json:object(), kapps_call:call()) -> 'ok'.
maybe_set_conference_id(Request, Call) ->
    ConferenceId = kapps_call:custom_sip_header(<<"X-Conference-ID">>, Call),
    CCVs = kapps_call:custom_channel_vars(Call),
    case kz_term:is_ne_binary(ConferenceId) of
        'false' -> maybe_get_conference_account_id(Request, Call, CCVs);
        'true' ->
            Updates = [{<<"Conference-ID">>, ConferenceId}],
            maybe_get_conference_account_id(Request
                                           ,Call
                                           ,kz_json:set_values(Updates, CCVs)
                                           )
    end.

-spec maybe_get_conference_account_id(kz_json:object(), kapps_call:call(), kz_json:object()) -> 'ok'.
maybe_get_conference_account_id(Request, Call, CCVs) ->
    AccountId = kapps_call:custom_sip_header(<<"X-Conference-Account-ID">>, Call),
    case kz_term:is_ne_binary(AccountId) of
        'true' ->
            Updates = [{<<"Account-ID">>, AccountId}],
            send_route_response(Request
                               ,kapps_call:set_account_db(kzs_util:format_account_db(AccountId), Call)
                               ,kz_json:set_values(Updates, CCVs)
                               );
        'false' ->
            maybe_lookup_conference_account_id(Request, Call, CCVs)
    end.


-spec maybe_lookup_conference_account_id(kz_json:object(), kapps_call:call(), kz_json:object()) -> 'ok'.
maybe_lookup_conference_account_id(Request, Call, CCVs) ->
    Realm = kapps_call:request_realm(Call),
    case kapps_util:get_account_by_realm(Realm) of
        {'ok', AccountDb} ->
            lager:debug("found account db by realm ~s: ~s", [Realm, AccountDb]),
            Updates = [{<<"Account-ID">>, kzs_util:format_account_id(AccountDb)}],
            send_route_response(Request
                               ,kapps_call:set_account_db(AccountDb, Call)
                               ,kz_json:set_values(Updates, CCVs)
                               );
        {'multiples', [AccountDb|_]} ->
            lager:debug("found account db by realm ~s: ~s", [Realm, AccountDb]),
            Updates = [{<<"Account-ID">>, kzs_util:format_account_id(AccountDb)}],
            send_route_response(Request
                               ,kapps_call:set_account_db(AccountDb, Call)
                               ,kz_json:set_values(Updates, CCVs)
                               );
        {'error', _R} ->
            lager:debug("unable to find account for realm ~s: ~p"
                       ,[Realm, _R]
                       ),
            'undefined'
    end.

-spec send_route_response(kz_json:object(), kapps_call:call(), kz_json:object()) -> 'ok'.
send_route_response(Request, Call, CCVs) ->
    lager:info("conference knows how to route the call! sending park response"),
    Resp = props:filter_undefined([{?KEY_MSG_ID, kz_api:msg_id(Request)}
                                  ,{?KEY_MSG_REPLY_ID, kapps_call:call_id_direct(Call)}
                                  ,{<<"Routes">>, []}
                                  ,{<<"Method">>, <<"park">>}
                                  ,{<<"Custom-Channel-Vars">>, CCVs}
                                  | kz_api:default_headers(?APP_NAME, ?APP_VERSION)
                                  ]),
    ServerId = kz_api:server_id(Request),
    Publisher = fun(P) -> kapi_route:publish_resp(ServerId, P) end,
    case kz_amqp_worker:call(Resp
                            ,Publisher
                            ,fun kapi_route:win_v/1
                            ,?ROUTE_WIN_TIMEOUT
                            )
    of
        {'ok', RouteWin} -> route_win(kapps_call:from_route_win(RouteWin, Call));
        {'error', _E} ->
            lager:info("conference didn't received a route win, exiting : ~p", [_E])
    end.

-spec route_win(kapps_call:call()) -> 'ok'.
route_win(Call) ->
    lager:info("conference has received a route win"),
    kapps_call_command:answer(Call),
    ConferenceId = kz_json:get_ne_binary_value(<<"Conference-ID">>
                                              ,kapps_call:custom_channel_vars(Call)
                                              ),
    DiscoverReq =
        props:filter_undefined(
          [{<<"Call">>, kapps_call:to_json(Call)}
          ,{<<"Conference-ID">>, ConferenceId}
          ,{<<"Moderator">>, is_moderator(Call)}
          ,{<<"Play-Welcome">>, maybe_play_welcome(Call)}
          ,{<<"Play-Welcome-Media">>, get_welcome_media(Call)}
          ,{<<"Play-Exit-Tone">>, maybe_play_exit(Call)}
          ,{<<"Play-Entry-Tone">>, maybe_play_entry(Call)}
          ,{<<"End-On-Leave">>, is_endconf_on_leave(Call)}
          ,{<<"End-On-Last-Member-Leave">>, is_last_member_endconf(Call)}
          ,{<<"Participant-Join-Video-Muted">>, is_participant_join_video_muted(Call)}
          ,{<<"Moderator-Join-Deaf">>, is_moderator_join_deaf(Call)}
          ,{<<"Moderator-Join-Muted">>, is_moderator_join_muted(Call)}
          ,{<<"Member-Join-Deaf">>, is_member_join_deaf(Call)}
          ,{<<"Member-Join-Muted">>, is_member_join_muted(Call)}
          ,{<<"Entry-Pin">>, kapps_call:custom_sip_header(<<"X-Conference-Pin">>, Call)}
          ,{<<"Require-Pin">>, 'false'}
          | kz_api:default_headers(?APP_NAME, ?APP_VERSION)
          ]),
    conf_discovery_req:process_req(kz_json:from_list(DiscoverReq)).

-spec is_moderator(kapps_call:call()) -> boolean().
is_moderator(Call) ->
    kz_term:is_true(kapps_call:custom_sip_header(<<"X-Conference-Moderator">>, Call)).

-spec is_endconf_on_leave(kapps_call:call()) -> kz_term:api_boolean().
is_endconf_on_leave(Call) ->
    case kapps_call:custom_sip_header(<<"X-Conference-End-On-Leave">>, Call) of
        'undefined' ->
            'undefined';
        Value ->
            kz_term:is_true(Value)
    end.

-spec is_last_member_endconf(kapps_call:call()) -> kz_term:api_boolean().
is_last_member_endconf(Call) ->
    case kapps_call:custom_sip_header(<<"X-Conference-End-On-Last-Member-Leave">>, Call) of
        'undefined' ->
            'undefined';
        Value ->
            kz_term:is_true(Value)
    end.

-spec is_participant_join_video_muted(kapps_call:call()) -> kz_term:api_boolean().
is_participant_join_video_muted(Call) ->
    case kapps_call:custom_sip_header(<<"X-Conference-Participant-Join-Video-Muted">>, Call) of
        'undefined' ->
            'undefined';
        Value ->
            kz_term:is_true(Value)
    end.

-spec is_moderator_join_deaf(kapps_call:call()) -> kz_term:api_boolean().
is_moderator_join_deaf(Call) ->
    case kapps_call:custom_sip_header(<<"X-Conference-Moderator-Join-Deaf">>, Call) of
        'undefined' ->
            'undefined';
        Value ->
            kz_term:is_true(Value)
    end.

-spec is_moderator_join_muted(kapps_call:call()) -> kz_term:api_boolean().
is_moderator_join_muted(Call) ->
    case kapps_call:custom_sip_header(<<"X-Conference-Moderator-Join-Muted">>, Call) of
        'undefined' ->
            'undefined';
        Value ->
            kz_term:is_true(Value)
    end.

-spec is_member_join_deaf(kapps_call:call()) -> kz_term:api_boolean().
is_member_join_deaf(Call) ->
    case kapps_call:custom_sip_header(<<"X-Conference-Member-Join-Deaf">>, Call) of
        'undefined' ->
            'undefined';
        Value ->
            kz_term:is_true(Value)
    end.

-spec is_member_join_muted(kapps_call:call()) -> kz_term:api_boolean().
is_member_join_muted(Call) ->
    case kapps_call:custom_sip_header(<<"X-Conference-Member-Join-Muted">>, Call) of
        'undefined' ->
            'undefined';
        Value ->
            kz_term:is_true(Value)
    end.

-spec maybe_play_welcome(kapps_call:call()) -> boolean().
maybe_play_welcome(Call) ->
    PlayWelcome = kapps_call:custom_sip_header(<<"X-Conference-Play-Welcome">>, Call),
    case kz_term:is_ne_binary(PlayWelcome) of
        'true' -> kz_term:is_true(PlayWelcome);
        'false' -> 'undefined'
    end.

-spec get_welcome_media(kapps_call:call()) -> kz_term:ne_binary().
get_welcome_media(Call) ->
    WelcomeMedia = kapps_call:custom_sip_header(<<"X-Conference-Welcome-Media-ID">>, Call),
    case kz_term:is_ne_binary(WelcomeMedia) of
        'true' -> WelcomeMedia;
        'false' -> 'undefined'
    end.

-spec maybe_play_entry(kapps_call:call()) -> kz_term:api_boolean().
maybe_play_entry(Call) ->
    PlayEntry = kapps_call:custom_sip_header(<<"X-Conference-Play-Entry">>, Call),
    case kz_term:is_ne_binary(PlayEntry) of
        'true' -> kz_term:is_true(PlayEntry);
        'false' -> 'undefined'
    end.


-spec maybe_play_exit(kapps_call:call()) -> boolean().
maybe_play_exit(Call) ->
    PlayExit = kapps_call:custom_sip_header(<<"X-Conference-Play-Exit">>, Call),
    case kz_term:is_ne_binary(PlayExit) of
        'true' -> kz_term:is_true(PlayExit);
        'false' -> 'undefined'
    end.
