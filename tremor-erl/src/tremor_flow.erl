%% Copyright 2022, The Tremor Team
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(tremor_flow).

-define(API_VERSION, "v1").
-define(ENDPOINT, "flows").
-export([ list/1, get/2, pause/2, resume/2]).

%%------------------------------------------------------------------------
%% @doc
%% List all currently deployed flows
%% @end
%%------------------------------------------------------------------------
-spec list(tremor_api:connection()) -> {ok, JSON::binary()}.
list(Conn) ->
    tremor_http:get([?API_VERSION, $/, ?ENDPOINT], Conn).

%%------------------------------------------------------------------------
%% @doc
%% Get a flow identified by Alias
%% @end
%%------------------------------------------------------------------------
-spec get(Alias :: binary(), tremor_api:connection()) -> {ok, JSON::binary()}.
get(Alias, Conn) ->
    tremor_http:get([?API_VERSION, $/, ?ENDPOINT, $/, Alias], Conn).


%%------------------------------------------------------------------------
%% @doc
%% Pause a flow
%% @end
%%------------------------------------------------------------------------
-spec pause(Alias :: binary(), tremor_api:connection()) -> {ok, JSON::binary()}.
pause(Alias, Conn) ->
    tremor_http:patch([?API_VERSION, $/, ?ENDPOINT, $/, Alias], #{ status => <<"paused">> }, Conn).

%%------------------------------------------------------------------------
%% @doc
%% Resume a flow
%% @end
%%------------------------------------------------------------------------
-spec resume(Alias :: binary(), tremor_api:connection()) -> {ok, JSON::binary()}.
resume(Alias, Conn) ->
    tremor_http:patch([?API_VERSION, $/, ?ENDPOINT, $/, Alias], #{ status => <<"running">> }, Conn).



