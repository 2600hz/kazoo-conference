-ifndef(CONFERENCE_HRL).
-include_lib("kazoo_stdlib/include/kz_types.hrl").
-include_lib("kazoo_stdlib/include/kz_log.hrl").
-include_lib("kazoo_amqp/include/kz_api_literals.hrl").

-include_lib("kazoo_call/src/kapps_conference.hrl").

-define(APP_NAME, <<"conference">>).
-define(APP_VERSION, <<"4.0.0">>).

-define(CONFIG_CAT, <<"conferences">>).

-define(CACHE_NAME, 'conference_cache').

-define(DEFAULT_MAX_MEMBERS_MEDIA, <<"conf-max_participants">>).

-define(RESOURCE_TYPES_HANDLED, [<<"audio">>, <<"video">>]).

-define(COLLECT_PIN_DEFAULT_TIMEOUT, 5 * ?MILLISECONDS_IN_SECOND).
-define(COLLECT_NUMBER_DEFAULT_TIMEOUT, 5 * ?MILLISECONDS_IN_SECOND).

-define(DEFAULT_MIN_REQUIRED_AUTO_RECORDING_PARTICIPANTS, 2).

-define(AUTO_RECORD_PATH(AccountId), <<"/tmp/", AccountId/binary, "_${conference_name}_${strftime(%Y-%m-%d-%H-%M-%S)}.wav">>).

-define(CONFERENCE_HRL, 'true').
-endif.
