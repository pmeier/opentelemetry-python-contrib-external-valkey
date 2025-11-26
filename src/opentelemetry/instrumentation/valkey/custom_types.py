# Copyright The OpenTelemetry Authors
# Copyright 2025 Philip Meier
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

from typing import Any, Callable, TypeVar

import valkey.asyncio.client
import valkey.asyncio.cluster
import valkey.client
import valkey.cluster
import valkey.connection

from opentelemetry.trace import Span

RequestHook = Callable[
    [Span, valkey.connection.Connection, list[Any], dict[str, Any]], None
]
ResponseHook = Callable[[Span, valkey.connection.Connection, Any], None]

AsyncPipelineInstance = TypeVar(
    "AsyncPipelineInstance",
    valkey.asyncio.client.Pipeline,
    valkey.asyncio.cluster.ClusterPipeline,
)
AsyncValkeyInstance = TypeVar(
    "AsyncValkeyInstance", valkey.asyncio.Valkey, valkey.asyncio.ValkeyCluster
)
PipelineInstance = TypeVar(
    "PipelineInstance",
    valkey.client.Pipeline,
    valkey.cluster.ClusterPipeline,
)
ValkeyInstance = TypeVar(
    "ValkeyInstance", valkey.client.Valkey, valkey.cluster.ValkeyCluster
)
R = TypeVar("R")
