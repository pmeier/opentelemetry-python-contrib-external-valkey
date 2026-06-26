# Copyright The OpenTelemetry Authors
# Copyright 2025 Philip Meier
# SPDX-License-Identifier: Apache-2.0

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
