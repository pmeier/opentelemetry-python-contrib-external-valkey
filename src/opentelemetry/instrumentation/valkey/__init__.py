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

"""
Instrument `valkey`_ to report Valkey queries.

.. _valkey: https://pypi.org/project/valkey/


Instrument All Clients
----------------------

The easiest way to instrument all valkey client instances is by
``ValkeyInstrumentor().instrument()``:

.. code:: python

    from opentelemetry.instrumentation.valkey import ValkeyInstrumentor
    import valkey


    # Instrument valkey
    ValkeyInstrumentor().instrument()

    # This will report a span with the default settings
    client = valkey.StrictValkey(host="localhost", port=6379)
    client.get("my-key")

Async Valkey clients (i.e. ``valkey.asyncio.Valkey``) are also instrumented in the same way:

.. code:: python

    from opentelemetry.instrumentation.valkey import ValkeyInstrumentor
    import valkey.asyncio


    # Instrument valkey
    ValkeyInstrumentor().instrument()

    # This will report a span with the default settings
    async def valkey_get():
        client = valkey.asyncio.Valkey(host="localhost", port=6379)
        await client.get("my-key")

.. note::
    Calling the ``instrument`` method will instrument the client classes, so any client
    created after the ``instrument`` call will be instrumented. To instrument only a
    single client, use :func:`ValkeyInstrumentor.instrument_client` method.

Instrument Single Client
------------------------

The :func:`ValkeyInstrumentor.instrument_client` can instrument a connection instance. This is useful when there are multiple clients with a different valkey database index.
Or, you might have a different connection pool used for an application function you
don't want instrumented.

.. code:: python

    from opentelemetry.instrumentation.valkey import ValkeyInstrumentor
    import valkey

    instrumented_client = valkey.Valkey()
    not_instrumented_client = valkey.Valkey()

    # Instrument valkey
    ValkeyInstrumentor.instrument_client(client=instrumented_client)

    # This will report a span with the default settings
    instrumented_client.get("my-key")

    # This will not have a span
    not_instrumented_client.get("my-key")

.. warning::
    All client instances created after calling ``ValkeyInstrumentor().instrument`` will
    be instrumented. To avoid instrumenting all clients, use
    :func:`ValkeyInstrumentor.instrument_client` .

Request/Response Hooks
----------------------

.. code:: python

    from opentelemetry.instrumentation.valkey import ValkeyInstrumentor
    import valkey

    def request_hook(span, instance, args, kwargs):
        if span and span.is_recording():
            span.set_attribute("custom_user_attribute_from_request_hook", "some-value")

    def response_hook(span, instance, response):
        if span and span.is_recording():
            span.set_attribute("custom_user_attribute_from_response_hook", "some-value")

    # Instrument valkey with hooks
    ValkeyInstrumentor().instrument(request_hook=request_hook, response_hook=response_hook)

    # This will report a span with the default settings and the custom attributes added from the hooks
    client = valkey.StrictValkey(host="localhost", port=6379)
    client.get("my-key")

Suppress Instrumentation
------------------------

You can use the ``suppress_instrumentation`` context manager to prevent instrumentation
from being applied to specific Valkey operations. This is useful when you want to avoid
creating spans for internal operations, health checks, or during specific code paths.

.. code:: python

    from opentelemetry.instrumentation.valkey import ValkeyInstrumentor
    from opentelemetry.instrumentation.utils import suppress_instrumentation
    import valkey

    # Instrument valkey
    ValkeyInstrumentor().instrument()

    client = valkey.StrictValkey(host="localhost", port=6379)

    # This will report a span
    client.get("my-key")

    # This will NOT report a span
    with suppress_instrumentation():
        client.get("internal-key")
        client.set("cache-key", "value")

    # This will report a span again
    client.get("another-key")

API
---
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Callable, Collection

import valkey
from wrapt import wrap_function_wrapper

from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.valkey.package import _instruments
from opentelemetry.instrumentation.valkey.util import (
    _add_create_attributes,
    _add_search_attributes,
    _build_span_meta_data_for_pipeline,
    _build_span_name,
    _format_command_args,
    _set_connection_attributes,
)
from opentelemetry.instrumentation.valkey.version import __version__
from opentelemetry.instrumentation.utils import (
    is_instrumentation_enabled,
    unwrap,
)
from opentelemetry.semconv._incubating.attributes.db_attributes import (
    DB_STATEMENT,
)
from opentelemetry.trace import (
    StatusCode,
    Tracer,
    TracerProvider,
    get_tracer,
)

if TYPE_CHECKING:
    from typing import Awaitable

    import valkey.asyncio.client
    import valkey.asyncio.cluster
    import valkey.client
    import valkey.cluster
    import valkey.connection

    from opentelemetry.instrumentation.valkey.custom_types import (
        AsyncPipelineInstance,
        AsyncValkeyInstance,
        PipelineInstance,
        R,
        ValkeyInstance,
        RequestHook,
        ResponseHook,
    )


_logger = logging.getLogger(__name__)

_REDIS_ASYNCIO_VERSION = (4, 2, 0)
_REDIS_CLUSTER_VERSION = (4, 1, 0)
_REDIS_ASYNCIO_CLUSTER_VERSION = (4, 3, 2)


_CLIENT_ASYNCIO_SUPPORT = valkey.VERSION >= _REDIS_ASYNCIO_VERSION
_CLIENT_ASYNCIO_CLUSTER_SUPPORT = (
    valkey.VERSION >= _REDIS_ASYNCIO_CLUSTER_VERSION
)
_CLIENT_CLUSTER_SUPPORT = valkey.VERSION >= _REDIS_CLUSTER_VERSION
_CLIENT_BEFORE_V3 = valkey.VERSION < (3, 0, 0)

if _CLIENT_ASYNCIO_SUPPORT:
    import valkey.asyncio

_INSTRUMENTATION_ATTR = "_is_instrumented_by_opentelemetry"


def _traced_execute_factory(
    tracer: Tracer,
    request_hook: RequestHook | None = None,
    response_hook: ResponseHook | None = None,
):
    def _traced_execute_command(
        func: Callable[..., R],
        instance: ValkeyInstance,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> R:
        if not is_instrumentation_enabled():
            return func(*args, **kwargs)

        query = _format_command_args(args)
        name = _build_span_name(instance, args)
        with tracer.start_as_current_span(
            name, kind=trace.SpanKind.CLIENT
        ) as span:
            if span.is_recording():
                span.set_attribute(DB_STATEMENT, query)
                _set_connection_attributes(span, instance)
                span.set_attribute("db.valkey.args_length", len(args))
                if span.name == "valkey.create_index":
                    _add_create_attributes(span, args)
            if callable(request_hook):
                request_hook(span, instance, args, kwargs)
            response = func(*args, **kwargs)
            if span.is_recording():
                if span.name == "valkey.search":
                    _add_search_attributes(span, response, args)
            if callable(response_hook):
                response_hook(span, instance, response)
            return response

    return _traced_execute_command


def _traced_execute_pipeline_factory(
    tracer: Tracer,
    request_hook: RequestHook | None = None,
    response_hook: ResponseHook | None = None,
):
    def _traced_execute_pipeline(
        func: Callable[..., R],
        instance: PipelineInstance,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> R:
        if not is_instrumentation_enabled():
            return func(*args, **kwargs)

        (
            command_stack,
            resource,
            span_name,
        ) = _build_span_meta_data_for_pipeline(instance)
        exception = None
        with tracer.start_as_current_span(
            span_name, kind=trace.SpanKind.CLIENT
        ) as span:
            if span.is_recording():
                span.set_attribute(DB_STATEMENT, resource)
                _set_connection_attributes(span, instance)
                span.set_attribute(
                    "db.valkey.pipeline_length", len(command_stack)
                )

            response = None
            try:
                response = func(*args, **kwargs)
            except valkey.WatchError as watch_exception:
                span.set_status(StatusCode.UNSET)
                exception = watch_exception

            if callable(response_hook):
                response_hook(span, instance, response)

        if exception:
            raise exception

        return response

    return _traced_execute_pipeline


def _async_traced_execute_factory(
    tracer: Tracer,
    request_hook: RequestHook | None = None,
    response_hook: ResponseHook | None = None,
):
    async def _async_traced_execute_command(
        func: Callable[..., Awaitable[R]],
        instance: AsyncValkeyInstance,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Awaitable[R]:
        if not is_instrumentation_enabled():
            return await func(*args, **kwargs)

        query = _format_command_args(args)
        name = _build_span_name(instance, args)

        with tracer.start_as_current_span(
            name, kind=trace.SpanKind.CLIENT
        ) as span:
            if span.is_recording():
                span.set_attribute(DB_STATEMENT, query)
                _set_connection_attributes(span, instance)
                span.set_attribute("db.valkey.args_length", len(args))
            if callable(request_hook):
                request_hook(span, instance, args, kwargs)
            response = await func(*args, **kwargs)
            if callable(response_hook):
                response_hook(span, instance, response)
            return response

    return _async_traced_execute_command


def _async_traced_execute_pipeline_factory(
    tracer: Tracer,
    request_hook: RequestHook | None = None,
    response_hook: ResponseHook | None = None,
):
    async def _async_traced_execute_pipeline(
        func: Callable[..., Awaitable[R]],
        instance: AsyncPipelineInstance,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Awaitable[R]:
        if not is_instrumentation_enabled():
            return await func(*args, **kwargs)

        (
            command_stack,
            resource,
            span_name,
        ) = _build_span_meta_data_for_pipeline(instance)

        exception = None

        with tracer.start_as_current_span(
            span_name, kind=trace.SpanKind.CLIENT
        ) as span:
            if span.is_recording():
                span.set_attribute(DB_STATEMENT, resource)
                _set_connection_attributes(span, instance)
                span.set_attribute(
                    "db.valkey.pipeline_length", len(command_stack)
                )

            response = None
            try:
                response = await func(*args, **kwargs)
            except valkey.WatchError as watch_exception:
                span.set_status(StatusCode.UNSET)
                exception = watch_exception

            if callable(response_hook):
                response_hook(span, instance, response)

        if exception:
            raise exception

        return response

    return _async_traced_execute_pipeline


# pylint: disable=R0915
def _instrument(
    tracer: Tracer,
    request_hook: RequestHook | None = None,
    response_hook: ResponseHook | None = None,
):
    _traced_execute_command = _traced_execute_factory(
        tracer, request_hook, response_hook
    )
    _traced_execute_pipeline = _traced_execute_pipeline_factory(
        tracer, request_hook, response_hook
    )
    pipeline_class = "BasePipeline" if _CLIENT_BEFORE_V3 else "Pipeline"
    valkey_class = "StrictValkey" if _CLIENT_BEFORE_V3 else "Valkey"

    wrap_function_wrapper(
        "valkey", f"{valkey_class}.execute_command", _traced_execute_command
    )
    wrap_function_wrapper(
        "valkey.client",
        f"{pipeline_class}.execute",
        _traced_execute_pipeline,
    )
    wrap_function_wrapper(
        "valkey.client",
        f"{pipeline_class}.immediate_execute_command",
        _traced_execute_command,
    )
    if _CLIENT_CLUSTER_SUPPORT:
        wrap_function_wrapper(
            "valkey.cluster",
            "ValkeyCluster.execute_command",
            _traced_execute_command,
        )
        wrap_function_wrapper(
            "valkey.cluster",
            "ClusterPipeline.execute",
            _traced_execute_pipeline,
        )

    _async_traced_execute_command = _async_traced_execute_factory(
        tracer, request_hook, response_hook
    )
    _async_traced_execute_pipeline = _async_traced_execute_pipeline_factory(
        tracer, request_hook, response_hook
    )
    if _CLIENT_ASYNCIO_SUPPORT:
        wrap_function_wrapper(
            "valkey.asyncio",
            f"{valkey_class}.execute_command",
            _async_traced_execute_command,
        )
        wrap_function_wrapper(
            "valkey.asyncio.client",
            f"{pipeline_class}.execute",
            _async_traced_execute_pipeline,
        )
        wrap_function_wrapper(
            "valkey.asyncio.client",
            f"{pipeline_class}.immediate_execute_command",
            _async_traced_execute_command,
        )
    if _CLIENT_ASYNCIO_CLUSTER_SUPPORT:
        wrap_function_wrapper(
            "valkey.asyncio.cluster",
            "ValkeyCluster.execute_command",
            _async_traced_execute_command,
        )
        wrap_function_wrapper(
            "valkey.asyncio.cluster",
            "ClusterPipeline.execute",
            _async_traced_execute_pipeline,
        )


def _instrument_client(
    client,
    tracer: Tracer,
    request_hook: RequestHook | None = None,
    response_hook: ResponseHook | None = None,
):
    # first, handle async clients and cluster clients
    _async_traced_execute = _async_traced_execute_factory(
        tracer, request_hook, response_hook
    )
    _async_traced_execute_pipeline = _async_traced_execute_pipeline_factory(
        tracer, request_hook, response_hook
    )

    if _CLIENT_ASYNCIO_SUPPORT and isinstance(client, valkey.asyncio.Valkey):

        def _async_pipeline_wrapper(func, instance, args, kwargs):
            result = func(*args, **kwargs)
            wrap_function_wrapper(
                result, "execute", _async_traced_execute_pipeline
            )
            wrap_function_wrapper(
                result, "immediate_execute_command", _async_traced_execute
            )
            return result

        wrap_function_wrapper(client, "execute_command", _async_traced_execute)
        wrap_function_wrapper(client, "pipeline", _async_pipeline_wrapper)
        return

    if _CLIENT_ASYNCIO_CLUSTER_SUPPORT and isinstance(
        client, valkey.asyncio.ValkeyCluster
    ):

        def _async_cluster_pipeline_wrapper(func, instance, args, kwargs):
            result = func(*args, **kwargs)
            wrap_function_wrapper(
                result, "execute", _async_traced_execute_pipeline
            )
            return result

        wrap_function_wrapper(client, "execute_command", _async_traced_execute)
        wrap_function_wrapper(
            client, "pipeline", _async_cluster_pipeline_wrapper
        )
        return
    # for valkey.client.Valkey, valkey.Cluster and v3.0.0 valkey.client.StrictValkey
    # the wrappers are the same
    _traced_execute = _traced_execute_factory(
        tracer, request_hook, response_hook
    )
    _traced_execute_pipeline = _traced_execute_pipeline_factory(
        tracer, request_hook, response_hook
    )

    def _pipeline_wrapper(func, instance, args, kwargs):
        result = func(*args, **kwargs)
        wrap_function_wrapper(result, "execute", _traced_execute_pipeline)
        wrap_function_wrapper(
            result, "immediate_execute_command", _traced_execute
        )
        return result

    wrap_function_wrapper(
        client,
        "execute_command",
        _traced_execute,
    )
    wrap_function_wrapper(
        client,
        "pipeline",
        _pipeline_wrapper,
    )


class ValkeyInstrumentor(BaseInstrumentor):
    @staticmethod
    def _get_tracer(**kwargs):
        tracer_provider = kwargs.get("tracer_provider")
        return get_tracer(
            __name__,
            __version__,
            tracer_provider=tracer_provider,
            schema_url="https://opentelemetry.io/schemas/1.11.0",
        )

    def instrument(
        self,
        tracer_provider: TracerProvider | None = None,
        request_hook: RequestHook | None = None,
        response_hook: ResponseHook | None = None,
        **kwargs,
    ):
        """Instruments all Valkey/StrictValkey/ValkeyCluster and async client instances.

        Args:
            tracer_provider: A TracerProvider, defaults to global.
            request_hook:
                a function with extra user-defined logic to run before performing the request.

                The ``args`` is a tuple, where items are
                command arguments. For example ``client.set("mykey", "value", ex=5)`` would
                have ``args`` as ``('SET', 'mykey', 'value', 'EX', 5)``.

                The ``kwargs`` represents occasional ``options`` passed by valkey. For example,
                if you use ``client.set("mykey", "value", get=True)``, the ``kwargs`` would be
                ``{'get': True}``.
            response_hook:
                a function with extra user-defined logic to run after the request is complete.

                The ``args`` represents the response.
        """
        super().instrument(
            tracer_provider=tracer_provider,
            request_hook=request_hook,
            response_hook=response_hook,
            **kwargs,
        )

    def _instrument(self, **kwargs: Any):
        """Instruments the valkey module

        Args:
            **kwargs: Optional arguments
                ``tracer_provider``: a TracerProvider, defaults to global.
                ``request_hook``: An optional callback that is invoked right after a span is created.
                ``response_hook``: An optional callback which is invoked right before the span is finished processing a response.
        """
        _instrument(
            self._get_tracer(**kwargs),
            request_hook=kwargs.get("request_hook"),
            response_hook=kwargs.get("response_hook"),
        )

    def _uninstrument(self, **kwargs: Any):
        if _CLIENT_BEFORE_V3:
            unwrap(valkey.StrictValkey, "execute_command")
            unwrap(valkey.StrictValkey, "pipeline")
            unwrap(valkey.Valkey, "pipeline")
            unwrap(
                valkey.client.BasePipeline,  # pylint:disable=no-member
                "execute",
            )
            unwrap(
                valkey.client.BasePipeline,  # pylint:disable=no-member
                "immediate_execute_command",
            )
        else:
            unwrap(valkey.Valkey, "execute_command")
            unwrap(valkey.Valkey, "pipeline")
            unwrap(valkey.client.Pipeline, "execute")
            unwrap(valkey.client.Pipeline, "immediate_execute_command")
        if _CLIENT_CLUSTER_SUPPORT:
            unwrap(valkey.cluster.ValkeyCluster, "execute_command")
            unwrap(valkey.cluster.ClusterPipeline, "execute")
        if _CLIENT_ASYNCIO_SUPPORT:
            unwrap(valkey.asyncio.Valkey, "execute_command")
            unwrap(valkey.asyncio.Valkey, "pipeline")
            unwrap(valkey.asyncio.client.Pipeline, "execute")
            unwrap(valkey.asyncio.client.Pipeline, "immediate_execute_command")
        if _CLIENT_ASYNCIO_CLUSTER_SUPPORT:
            unwrap(valkey.asyncio.cluster.ValkeyCluster, "execute_command")
            unwrap(valkey.asyncio.cluster.ClusterPipeline, "execute")

    @staticmethod
    def instrument_client(
        client: valkey.StrictValkey
        | valkey.Valkey
        | valkey.asyncio.Valkey
        | valkey.cluster.ValkeyCluster
        | valkey.asyncio.cluster.ValkeyCluster,
        tracer_provider: TracerProvider | None = None,
        request_hook: RequestHook | None = None,
        response_hook: ResponseHook | None = None,
    ):
        """Instrument the provided Valkey Client. The client can be sync or async.
        Cluster client is also supported.

        Args:
            client: The valkey client.
            tracer_provider: A TracerProvider, defaults to global.
            request_hook: a function with extra user-defined logic to run before
                performing the request.

                The ``args`` is a tuple, where items are
                command arguments. For example ``client.set("mykey", "value", ex=5)`` would
                have ``args`` as ``('SET', 'mykey', 'value', 'EX', 5)``.

                The ``kwargs`` represents occasional ``options`` passed by valkey. For example,
                if you use ``client.set("mykey", "value", get=True)``, the ``kwargs`` would be
                ``{'get': True}``.

            response_hook: a function with extra user-defined logic to run after
                the request is complete.

                The ``args`` represents the response.
        """
        if not hasattr(client, _INSTRUMENTATION_ATTR):
            setattr(client, _INSTRUMENTATION_ATTR, False)
        if not getattr(client, _INSTRUMENTATION_ATTR):
            _instrument_client(
                client,
                ValkeyInstrumentor._get_tracer(tracer_provider=tracer_provider),
                request_hook=request_hook,
                response_hook=response_hook,
            )
            setattr(client, _INSTRUMENTATION_ATTR, True)
        else:
            _logger.warning(
                "Attempting to instrument Valkey connection while already instrumented"
            )

    @staticmethod
    def uninstrument_client(
        client: valkey.StrictValkey
        | valkey.Valkey
        | valkey.asyncio.Valkey
        | valkey.cluster.ValkeyCluster
        | valkey.asyncio.cluster.ValkeyCluster,
    ):
        """Disables instrumentation for the given client instance

        Args:
            client: The valkey client
        """
        if getattr(client, _INSTRUMENTATION_ATTR):
            # for all clients we need to unwrap execute_command and pipeline functions
            unwrap(client, "execute_command")
            # the method was creating a pipeline and wrapping the functions of the
            # created instance. any pipelines created before un-instrumenting will
            # remain instrumented (pipelines should usually have a short span)
            unwrap(client, "pipeline")
        else:
            _logger.warning(
                "Attempting to un-instrument Valkey connection that wasn't instrumented"
            )
            return

    def instrumentation_dependencies(self) -> Collection[str]:
        """Return a list of python packages with versions that the will be instrumented."""
        return _instruments
