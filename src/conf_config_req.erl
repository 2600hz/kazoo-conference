%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2011-2020, 2600Hz
%%% @doc
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(conf_config_req).

-export([handle_req/2
        ,cache_profile/1
        ]).

-include("conference.hrl").
-include_lib("kazoo_stdlib/include/kz_databases.hrl").

-spec cache_profile(kapps_conference:conference()) -> 'ok'.
cache_profile(Conference) ->
    Name = kapps_conference:profile_name(Conference),
    case lookup_profile(Name, Conference) of
        {ok, _} -> ok;
        {error, not_found} ->
            Profile = build_profile(Conference, Name),
            cache_profile(Conference, Name, Profile)
    end.

cache_profile(Conference, Name, Profile) ->
    lager:debug("caching profile ~s: ~p", [Name, Profile]),
    CacheProps = [{'origin', cache_origin(Conference)}],
    Key = {'profile', kapps_conference:account_id(Conference), kapps_conference:id(Conference), Name},
    kz_cache:store_local(?CACHE_NAME, Key, Profile, CacheProps).

-spec cache_origin(kapps_conference:conference()) -> list().
cache_origin(Conference) ->
    case kapps_conference:id(Conference) of
        undefined -> [];
        ConferenceId ->
            AccountId = kapps_conference:account_id(Conference),
            AccountDB = kzs_util:format_account_db(AccountId),
            [{'db', AccountDB, ConferenceId}]
    end.

-spec handle_req(kz_json:object(), kz_term:proplist()) -> 'ok'.
handle_req(JObj, _Props) ->
    'true' = kapi_conference:config_req_v(JObj),
    Request = kz_json:get_ne_binary_value(<<"Request">>, JObj),
    lager:debug("'~s' profile request received", [Request]),
    handle_request(Request, JObj, create_conference(JObj)).

-spec create_conference(kz_json:object()) -> kapps_conference:conference().
create_conference(JObj) ->
    Conference = kapps_conference:new(),
    Profile = kz_json:get_ne_binary_value(<<"Profile">>, JObj),
    AccountId = kz_json:get_ne_binary_value(<<"Account-ID">>, JObj),
    ConferenceId = kz_json:get_ne_binary_value(<<"Conference-ID">>, JObj),
    lager:debug("creating conference config for ~s in account ~s", [ConferenceId, AccountId]),
    Routines = [{fun kapps_conference:set_account_id/2, AccountId}
               ,{fun kapps_conference:set_id/2, ConferenceId}
               ,{fun kapps_conference:set_profile_name/2, Profile}
               ,fun kapps_conference:reload/1
               ],
    kapps_conference:update(Routines, Conference).

-spec handle_request(kz_term:ne_binary(), kz_json:object(), kapps_conference:conference()) -> 'ok'.
handle_request(<<"Conference">>, JObj, Conference) ->
    handle_profile_request(JObj, Conference);
handle_request(<<"Controls">>, JObj, Conference) ->
    handle_controls_request(JObj, Conference).

-spec handle_profile_request(kz_json:object(), kapps_conference:conference()) -> 'ok'.
handle_profile_request(JObj, Conference) ->
    ProfileName = requested_profile_name(JObj),
    Profile = fetch_profile(ProfileName, Conference),
    Controls = conference_controls(Conference),

    ServerId = kz_api:server_id(JObj),
    Resp = [{<<"Profiles">>, kz_json:from_list([{ProfileName, Profile}])}
           ,{<<"Caller-Controls">>, Controls}
           ,{<<"Advertise">>, advertise(ProfileName)}
           ,{<<"Chat-Permissions">>, chat_permissions(ProfileName)}
           ,{<<"Msg-ID">>, kz_api:msg_id(JObj)}
           | kz_api:default_headers(?APP_NAME, ?APP_VERSION)
           ],
    lager:debug("returning conference profile ~s", [ProfileName]),
    lager:debug("~s", [kz_json:encode(kz_json:from_list(Resp))]),
    kapi_conference:publish_config_resp(ServerId, props:filter_undefined(Resp)).

-spec fetch_profile(kz_term:ne_binary(), kapps_conference:conference()) -> kz_json:object().
fetch_profile(ProfileName, Conference) ->
    case lookup_profile(ProfileName, Conference) of
        {'ok', Profile} -> Profile;
        {'error', 'not_found'} ->
            Profile = build_profile(Conference, ProfileName),
            cache_profile(Conference, ProfileName, Profile),
            Profile
    end.

-spec lookup_profile(kz_term:ne_binary(), kapps_conference:conference()) ->
          {'ok', kz_json:object()} |
          {'error', 'not_found'}.
lookup_profile(ProfileName, Conference) ->
    Key = {'profile', kapps_conference:account_id(Conference), kapps_conference:id(Conference), ProfileName},
    lager:info("looking up profile ~s", [ProfileName]),
    kz_cache:peek_local(?CACHE_NAME, Key).

-spec requested_profile_name(kz_json:object()) -> kz_term:ne_binary().
requested_profile_name(JObj) ->
    kz_json:get_ne_binary_value(<<"Profile">>, JObj, ?DEFAULT_PROFILE_NAME).

-spec advertise(kz_term:ne_binary()) -> kz_term:api_object().
advertise(?DEFAULT_PROFILE_NAME = ProfileName) ->
    advertise(ProfileName, ?ADVERTISE(ProfileName, ?DEFAULT_ADVERTISE_CONFIG));
advertise(?PAGE_PROFILE_NAME = ProfileName) ->
    advertise(ProfileName, ?ADVERTISE(ProfileName, ?PAGE_ADVERTISE_CONFIG));
advertise(ProfileName) ->
    advertise(ProfileName, ?ADVERTISE(ProfileName)).

-spec advertise(kz_term:ne_binary(), kz_term:api_object()) -> kz_term:api_object().
advertise(_ProfileName, 'undefined') -> 'undefined';
advertise(ProfileName, Advertise) -> kz_json:from_list([{ProfileName, Advertise}]).

-spec chat_permissions(kz_term:ne_binary()) -> kz_term:api_object().
chat_permissions(?DEFAULT_PROFILE_NAME = ProfileName) ->
    chat_permissions(ProfileName, ?CHAT_PERMISSIONS(ProfileName, ?DEFAULT_CHAT_CONFIG));
chat_permissions(?PAGE_PROFILE_NAME= ProfileName) ->
    chat_permissions(ProfileName, ?CHAT_PERMISSIONS(ProfileName, ?PAGE_CHAT_CONFIG));
chat_permissions(ProfileName) ->
    chat_permissions(ProfileName, ?CHAT_PERMISSIONS(ProfileName)).

-spec chat_permissions(kz_term:ne_binary(), kz_term:api_object()) -> kz_term:api_object().
chat_permissions(_ProfileName, 'undefined') -> 'undefined';
chat_permissions(ProfileName, Chat) -> kz_json:from_list([{ProfileName, Chat}]).

-spec build_profile(kapps_conference:conference(), kz_term:ne_binary()) -> kz_json:object().
build_profile(Conference, Name) ->
    {PName, Profile} = kapps_conference:profile(kapps_conference:set_profile_name(Name, Conference)),
    case PName of
        Name -> 'ok';
        _ -> lager:debug("~p profile not found, using ~p instead", [Name, PName])
    end,
    lager:info("built profile ~s: ~p", [PName, Profile]),
    build_profile_routines(Conference, Profile).

-spec build_profile_routines(kapps_conference:conference(), kz_json:object()) -> kz_json:object().
build_profile_routines(Conference, Profile) ->
    Routines = [fun add_conference_params/2
               ,fun add_name_announcement/2
               ,fun add_on_enter_extension/2
               ,fun verify_entry_tones/2
               ,fun verify_exit_tones/2
               ],
    lists:foldl(fun(F, P) -> F(Conference, P) end, Profile, Routines).

-spec add_name_announcement(kapps_conference:conference(), kz_json:object()) -> kz_json:object().
add_name_announcement(Conference, Profile) ->
    case kapps_conference:play_name_on_join(Conference)
        andalso ?SUPPORT_NAME_ANNOUNCEMENT(kapps_conference:account_id(Conference))
    of
        'true' -> kz_json:set_value([<<"conference-variables">>, <<"conference-name-announcement">>], 'true', Profile);
        'false' -> Profile
    end.

-spec add_on_enter_extension(kapps_conference:conference(), kz_json:object()) -> kz_json:object().
add_on_enter_extension(Conference, Profile) ->
    case kzd_conferences:on_enter_extension(kapps_conference:conference_doc(Conference)) of
        undefined -> Profile;
        Extension -> kz_json:set_value(<<"on-enter-extension">>, Extension, Profile)
    end.

-spec add_conference_params(kapps_conference:conference(), kz_json:object()) -> kz_json:object().
add_conference_params(Conference, Profile) ->
    Props = props:filter_undefined(
              [{<<"max-members">>, max_participants(Conference)}
              ,{<<"max-members-sound">>, max_members_sound(Conference)}
              ,{<<"caller-controls">>, kapps_conference:caller_controls(Conference)}
              ,{<<"moderator-controls">>, kapps_conference:moderator_controls(Conference)}
              ,{<<"member-flags">>, member_flags(Conference)}
              ,{<<"domain">>, kapps_conference:domain(Conference)}
              ,{<<"extra-settings">>, kapps_conference:extra_settings(Conference)}
              ,{<<"verbose-events">>, true}
              ]),
    kz_json:set_values(Props, Profile).

-spec verify_entry_tones(kapps_conference:conference(), kz_json:object()) -> kz_json:object().
verify_entry_tones(Conference, Profile) ->
    Key = <<"enter-sound">>,
    case kapps_conference:play_entry_tone(Conference) of
        'true' -> ensure_tone(Key, Profile, kapps_conference:entry_tone(Conference));
        'false' -> remove_tone(Key, Profile)
    end.

-spec verify_exit_tones(kapps_conference:conference(), kz_json:object()) -> kz_json:object().
verify_exit_tones(Conference, Profile) ->
    Key = <<"exit-sound">>,
    case kapps_conference:play_exit_tone(Conference) of
        'true' -> ensure_tone(Key, Profile, kapps_conference:exit_tone(Conference));
        'false' -> remove_tone(Key, Profile)
    end.

-spec ensure_tone(kz_term:ne_binary(), kz_json:object(), kz_term:ne_binary()) -> kz_json:object().
ensure_tone(Key, Profile, Tone) ->
    case kz_json:get_ne_value(Key, Profile) of
        'undefined' ->
            lager:debug("ensure tone adding ~s ~s", [Key, Tone]),
            kz_json:set_value(Key, Tone, Profile);
        _Else ->
            lager:debug("ensure tone already has ~s", [Key]),
            Profile
    end.

-spec remove_tone(kz_term:ne_binary(), kz_json:object()) -> kz_json:object().
remove_tone(Key, Profile) ->
    lager:debug("remove tone ~s", [Key]),
    kz_json:delete_key(Key, Profile).

-spec max_participants(kapps_conference:conference()) -> kz_term:api_binary().
max_participants(Conference) ->
    case kapps_conference:max_participants(Conference) of
        N when is_integer(N), N > 1 -> N;
        _Else -> 'undefined'
    end.

-spec max_members_sound(kapps_conference:conference()) -> kz_term:api_binary().
max_members_sound(Conference) ->
    AccountId = kapps_conference:account_id(Conference),
    case kapps_conference:max_members_media(Conference) of
        'undefined' when 'undefined' =:= AccountId ->
            lager:debug("getting system max-members prompt"),
            kapps_prompt:get_prompt(AccountId, ?DEFAULT_MAX_MEMBERS_MEDIA, kapps_conference:language(Conference));
        'undefined' ->
            lager:debug("getting max members prompt from account ~s", [AccountId]),
            kapps_prompt:get_prompt(AccountId, ?DEFAULT_MAX_MEMBERS_MEDIA, kapps_conference:language(Conference));
        <<(Media):32/binary>> ->
            lager:debug("conference has max-members-sound: ~s", [Media]),
            kapps_prompt:get_prompt(AccountId, Media);
        Media ->
            lager:debug("conference has max-members-sound: ~s", [Media]),
            Media
    end.

-spec handle_controls_request(kz_json:object(), kapps_conference:conference()) -> 'ok'.
handle_controls_request(JObj, Conference) ->
    ProfileName = requested_profile_name(JObj),
    ControlsType = requested_controls_name(JObj),
    ControlsName = get_conference_controls_name(ControlsType, Conference),
    Controls = kapps_conference:controls(Conference, ControlsName),
    ServerId = kz_api:server_id(JObj),
    Resp = [{<<"Caller-Controls">>, controls(ControlsType, Controls)}
           ,{<<"Msg-ID">>, kz_api:msg_id(JObj)}
           | kz_api:default_headers(?APP_NAME, ?APP_VERSION)
           ],
    lager:debug("returning ~s (~s) controls profile for ~s"
               ,[ControlsName, ControlsType, ProfileName]
               ),
    kapi_conference:publish_config_resp(ServerId, Resp).

-spec requested_controls_name(kz_json:object()) -> kz_term:ne_binary().
requested_controls_name(JObj) ->
    kz_json:get_ne_value(<<"Controls">>, JObj).

-spec controls(kz_term:ne_binary(), kz_json:objects()) -> kz_json:object().
controls(ControlsName, Controls) ->
    kz_json:from_list([{ControlsName, Controls}]).

-spec get_conference_controls_name(kz_term:ne_binary(), kapps_conference:conference()) -> kz_term:ne_binary().
get_conference_controls_name(<<"caller-controls">>, Conference) ->
    kapps_conference:caller_controls(Conference);
get_conference_controls_name(<<"moderator-controls">>, Conference) ->
    kapps_conference:moderator_controls(Conference);
get_conference_controls_name(_Name, Conference) ->
    kapps_conference:caller_controls(Conference).

conference_controls(Conference) ->
    ControlNames = lists:usort([kapps_conference:caller_controls(Conference)
                               ,kapps_conference:moderator_controls(Conference)
                               ]),
    kz_json:from_list([{Name, kapps_conference:controls(Conference, Name)} || Name <- ControlNames]).

member_flags(Conference) ->
    IsMuted = kapps_conference:member_join_muted(Conference),
    IsDeaf = kapps_conference:member_join_deaf(Conference),
    member_flags(IsMuted, IsDeaf).

member_flags(false, false) -> undefined;
member_flags(true, true) -> <<"mute|deaf">>;
member_flags(true, false) -> <<"mute">>;
member_flags(false, true) -> <<"deaf">>.
