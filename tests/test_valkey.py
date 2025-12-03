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

import asyncio
from unittest import IsolatedAsyncioTestCase, mock
from unittest.mock import AsyncMock

import fakeredis
import pytest
import valkey
import valkey.asyncio
from fakeredis import FakeAsyncValkey
from redis.exceptions import ConnectionError as redis_ConnectionError
from valkey.exceptions import WatchError

from opentelemetry import trace
from opentelemetry.instrumentation.valkey import ValkeyInstrumentor
from opentelemetry.instrumentation.utils import suppress_instrumentation
from opentelemetry.semconv._incubating.attributes.db_attributes import (
    DB_REDIS_DATABASE_INDEX,
    DB_SYSTEM,
    DbSystemValues,
)
from opentelemetry.semconv._incubating.attributes.net_attributes import (
    NET_PEER_NAME,
    NET_PEER_PORT,
    NET_TRANSPORT,
    NetTransportValues,
)
from opentelemetry.test.test_base import TestBase
from opentelemetry.trace import SpanKind


# pylint: disable=too-many-public-methods
class TestValkey(TestBase):
    def assert_span_count(self, count: int):
        """
        Assert that the memory exporter has the expected number of spans.
        Returns the spans list if assertion passes
        """
        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), count)
        return spans

    def setUp(self):
        super().setUp()
        ValkeyInstrumentor().instrument(tracer_provider=self.tracer_provider)

    def tearDown(self):
        super().tearDown()
        ValkeyInstrumentor().uninstrument()

    def test_span_properties(self):
        valkey_client = valkey.Valkey()

        with mock.patch.object(valkey_client, "connection"):
            valkey_client.get("key")

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        span = spans[0]
        self.assertEqual(span.name, "GET")
        self.assertEqual(span.kind, SpanKind.CLIENT)

    def test_not_recording(self):
        valkey_client = valkey.Valkey()

        mock_tracer = mock.Mock()
        mock_span = mock.Mock()
        mock_span.is_recording.return_value = False
        mock_tracer.start_span.return_value = mock_span
        with mock.patch("opentelemetry.trace.get_tracer") as tracer:
            with mock.patch.object(valkey_client, "connection"):
                tracer.return_value = mock_tracer
                valkey_client.get("key")
                self.assertFalse(mock_span.is_recording())
                self.assertTrue(mock_span.is_recording.called)
                self.assertFalse(mock_span.set_attribute.called)
                self.assertFalse(mock_span.set_status.called)

    def test_instrument_uninstrument(self):
        valkey_client = valkey.Valkey()

        with mock.patch.object(valkey_client, "connection"):
            valkey_client.get("key")

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        self.memory_exporter.clear()

        # Test uninstrument
        ValkeyInstrumentor().uninstrument()

        with mock.patch.object(valkey_client, "connection"):
            valkey_client.get("key")

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 0)
        self.memory_exporter.clear()

        # Test instrument again
        ValkeyInstrumentor().instrument()

        with mock.patch.object(valkey_client, "connection"):
            valkey_client.get("key")

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)

    def test_instrument_uninstrument_async_client_command(self):
        valkey_client = valkey.asyncio.Valkey()

        with mock.patch.object(valkey_client, "connection", AsyncMock()):
            asyncio.run(valkey_client.get("key"))

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        self.memory_exporter.clear()

        # Test uninstrument
        ValkeyInstrumentor().uninstrument()

        with mock.patch.object(valkey_client, "connection", AsyncMock()):
            asyncio.run(valkey_client.get("key"))

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 0)
        self.memory_exporter.clear()

        # Test instrument again
        ValkeyInstrumentor().instrument()

        with mock.patch.object(valkey_client, "connection", AsyncMock()):
            asyncio.run(valkey_client.get("key"))

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)

    def test_response_hook(self):
        valkey_client = valkey.Valkey()
        connection = valkey.connection.Connection()
        valkey_client.connection = connection

        response_attribute_name = "db.valkey.response"

        def response_hook(span, conn, response):
            span.set_attribute(response_attribute_name, response)

        ValkeyInstrumentor().uninstrument()
        ValkeyInstrumentor().instrument(
            tracer_provider=self.tracer_provider, response_hook=response_hook
        )

        test_value = "test_value"

        with mock.patch.object(connection, "send_command"):
            with mock.patch.object(
                valkey_client, "parse_response", return_value=test_value
            ):
                valkey_client.get("key")

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)

        span = spans[0]
        self.assertEqual(
            span.attributes.get(response_attribute_name), test_value
        )

    def test_request_hook(self):
        valkey_client = valkey.Valkey()
        connection = valkey.connection.Connection()
        valkey_client.connection = connection

        custom_attribute_name = "my.request.attribute"

        def request_hook(span, conn, args, kwargs):
            if span and span.is_recording():
                span.set_attribute(custom_attribute_name, args[0])

        ValkeyInstrumentor().uninstrument()
        ValkeyInstrumentor().instrument(
            tracer_provider=self.tracer_provider, request_hook=request_hook
        )

        test_value = "test_value"

        with mock.patch.object(connection, "send_command"):
            with mock.patch.object(
                valkey_client, "parse_response", return_value=test_value
            ):
                valkey_client.get("key")

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)

        span = spans[0]
        self.assertEqual(span.attributes.get(custom_attribute_name), "GET")

    def test_query_sanitizer_enabled(self):
        valkey_client = valkey.Valkey()
        connection = valkey.connection.Connection()
        valkey_client.connection = connection

        ValkeyInstrumentor().uninstrument()
        ValkeyInstrumentor().instrument(
            tracer_provider=self.tracer_provider,
            sanitize_query=True,
        )

        with mock.patch.object(valkey_client, "connection"):
            valkey_client.set("key", "value")

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)

        span = spans[0]
        self.assertEqual(span.attributes.get("db.statement"), "SET ? ?")

    def test_query_sanitizer(self):
        valkey_client = valkey.Valkey()
        connection = valkey.connection.Connection()
        valkey_client.connection = connection

        with mock.patch.object(valkey_client, "connection"):
            valkey_client.set("key", "value")

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)

        span = spans[0]
        self.assertEqual(span.attributes.get("db.statement"), "SET ? ?")

    def test_no_op_tracer_provider(self):
        ValkeyInstrumentor().uninstrument()
        tracer_provider = trace.NoOpTracerProvider()
        ValkeyInstrumentor().instrument(tracer_provider=tracer_provider)

        valkey_client = valkey.Valkey()

        with mock.patch.object(valkey_client, "connection"):
            valkey_client.get("key")

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 0)

    def test_attributes_default(self):
        valkey_client = valkey.Valkey()

        with mock.patch.object(valkey_client, "connection"):
            valkey_client.set("key", "value")

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)

        span = spans[0]
        self.assertEqual(
            span.attributes[DB_SYSTEM],
            DbSystemValues.REDIS.value,
        )
        self.assertEqual(span.attributes[DB_REDIS_DATABASE_INDEX], 0)
        self.assertEqual(span.attributes[NET_PEER_NAME], "localhost")
        self.assertEqual(span.attributes[NET_PEER_PORT], 6379)
        self.assertEqual(
            span.attributes[NET_TRANSPORT],
            NetTransportValues.IP_TCP.value,
        )

    def test_attributes_tcp(self):
        valkey_client = valkey.Valkey.from_url("valkey://foo:bar@1.1.1.1:6380/1")

        with mock.patch.object(valkey_client, "connection"):
            valkey_client.set("key", "value")

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)

        span = spans[0]
        self.assertEqual(
            span.attributes[DB_SYSTEM],
            DbSystemValues.REDIS.value,
        )
        self.assertEqual(span.attributes[DB_REDIS_DATABASE_INDEX], 1)
        self.assertEqual(span.attributes[NET_PEER_NAME], "1.1.1.1")
        self.assertEqual(span.attributes[NET_PEER_PORT], 6380)
        self.assertEqual(
            span.attributes[NET_TRANSPORT],
            NetTransportValues.IP_TCP.value,
        )

    def test_attributes_unix_socket(self):
        valkey_client = valkey.Valkey.from_url(
            "unix://foo@/path/to/socket.sock?db=3&password=bar"
        )

        with mock.patch.object(valkey_client, "connection"):
            valkey_client.set("key", "value")

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)

        span = spans[0]
        self.assertEqual(
            span.attributes[DB_SYSTEM],
            DbSystemValues.REDIS.value,
        )
        self.assertEqual(span.attributes[DB_REDIS_DATABASE_INDEX], 3)
        self.assertEqual(
            span.attributes[NET_PEER_NAME],
            "/path/to/socket.sock",
        )
        self.assertEqual(
            span.attributes[NET_TRANSPORT],
            NetTransportValues.OTHER.value,
        )

    def test_connection_error(self):
        server = fakeredis.FakeServer(server_type="valkey")
        server.connected = False
        valkey_client = fakeredis.FakeStrictValkey(server=server)
        try:
            valkey_client.set("foo", "bar")
        except redis_ConnectionError:
            pass

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        span = spans[0]

        self.assertEqual(span.name, "SET")
        self.assertEqual(span.kind, SpanKind.CLIENT)
        self.assertEqual(span.status.status_code, trace.StatusCode.ERROR)

    def test_response_error(self):
        valkey_client = fakeredis.FakeStrictValkey()
        valkey_client.lpush("mylist", "value")
        try:
            valkey_client.incr(
                "mylist"
            )  # Trying to increment a list, which is invalid
        except valkey.ResponseError:
            pass

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 2)

        span = spans[0]
        self.assertEqual(span.name, "LPUSH")
        self.assertEqual(span.kind, SpanKind.CLIENT)
        self.assertEqual(span.status.status_code, trace.StatusCode.UNSET)

        span = spans[1]
        self.assertEqual(span.name, "INCRBY")
        self.assertEqual(span.kind, SpanKind.CLIENT)
        self.assertEqual(span.status.status_code, trace.StatusCode.ERROR)

    def test_watch_error_sync(self):
        def redis_operations():
            with pytest.raises(WatchError):
                valkey_client = fakeredis.FakeStrictValkey()
                pipe = valkey_client.pipeline(transaction=True)
                pipe.watch("a")
                valkey_client.set("a", "bad")  # This will cause the WatchError
                pipe.multi()
                pipe.set("a", "1")
                pipe.execute()

        redis_operations()

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 3)

        # there should be 3 tests, we start watch operation and have 2 set operation on same key
        self.assertEqual(len(spans), 3)

        self.assertEqual(spans[0].attributes.get("db.statement"), "WATCH ?")
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)
        self.assertEqual(spans[0].status.status_code, trace.StatusCode.UNSET)

        for span in spans[1:]:
            self.assertEqual(span.attributes.get("db.statement"), "SET ? ?")
            self.assertEqual(span.kind, SpanKind.CLIENT)
            self.assertEqual(span.status.status_code, trace.StatusCode.UNSET)

    def test_span_name_empty_pipeline(self):
        valkey_client = fakeredis.FakeStrictValkey()
        pipe = valkey_client.pipeline()
        pipe.execute()

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0].name, "valkey")
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)
        self.assertEqual(spans[0].status.status_code, trace.StatusCode.UNSET)

    def test_suppress_instrumentation_command(self):
        valkey_client = valkey.Valkey()

        with mock.patch.object(valkey_client, "connection"):
            # Execute command with suppression
            with suppress_instrumentation():
                valkey_client.get("key")

        # No spans should be created
        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 0)

        # Verify that instrumentation works again after exiting the context
        with mock.patch.object(valkey_client, "connection"):
            valkey_client.get("key")

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)

    def test_suppress_instrumentation_pipeline(self):
        valkey_client = fakeredis.FakeStrictValkey()

        with suppress_instrumentation():
            pipe = valkey_client.pipeline()
            pipe.set("key1", "value1")
            pipe.set("key2", "value2")
            pipe.get("key1")
            pipe.execute()

        # No spans should be created
        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 0)

        # Verify that instrumentation works again after exiting the context
        pipe = valkey_client.pipeline()
        pipe.set("key3", "value3")
        pipe.execute()

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        # Pipeline span could be "SET" or "valkey.pipeline" depending on implementation
        self.assertIn(spans[0].name, ["SET", "valkey.pipeline"])

    def test_suppress_instrumentation_mixed(self):
        valkey_client = valkey.Valkey()

        # Regular instrumented call
        with mock.patch.object(valkey_client, "connection"):
            valkey_client.set("key1", "value1")

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        self.memory_exporter.clear()

        # Suppressed call
        with suppress_instrumentation():
            with mock.patch.object(valkey_client, "connection"):
                valkey_client.set("key2", "value2")

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 0)

        # Another regular instrumented call
        with mock.patch.object(valkey_client, "connection"):
            valkey_client.get("key1")

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)


class TestValkeyAsync(TestBase, IsolatedAsyncioTestCase):
    def assert_span_count(self, count: int):
        """
        Assert that the memory exporter has the expected number of spans.
        Returns the spans list if assertion passes
        """
        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), count)
        return spans

    def setUp(self):
        super().setUp()
        self.instrumentor = ValkeyInstrumentor()
        self.client: FakeAsyncValkey = FakeAsyncValkey()

    @staticmethod
    async def _redis_pipeline_operations(client: FakeAsyncValkey):
        with pytest.raises(WatchError):
            async with client.pipeline(transaction=False) as pipe:
                await pipe.watch("a")
                await client.set("a", "bad")
                pipe.multi()
                await pipe.set("a", "1")
                await pipe.execute()

    @pytest.mark.asyncio
    async def test_watch_error_async(self):
        # this tests also ensures the response_hook is called
        response_attr = "my.response.attribute"
        count = 0

        def response_hook(span, conn, args):
            nonlocal count
            if span and span.is_recording():
                span.set_attribute(response_attr, count)
                count += 1

        self.instrumentor.instrument(
            tracer_provider=self.tracer_provider, response_hook=response_hook
        )
        valkey_client = FakeAsyncValkey()
        await self._redis_pipeline_operations(valkey_client)

        # there should be 3 tests, we start watch operation and have 2 set operation on same key
        spans = self.assert_span_count(3)

        self.assertEqual(spans[0].attributes.get("db.statement"), "WATCH ?")
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)
        self.assertEqual(spans[0].status.status_code, trace.StatusCode.UNSET)
        self.assertEqual(spans[0].attributes.get(response_attr), 0)

        for span_index, span in enumerate(spans[1:], 1):
            self.assertEqual(span.attributes.get("db.statement"), "SET ? ?")
            self.assertEqual(span.kind, SpanKind.CLIENT)
            self.assertEqual(span.status.status_code, trace.StatusCode.UNSET)
            self.assertEqual(span.attributes.get(response_attr), span_index)
        ValkeyInstrumentor().uninstrument()

    @pytest.mark.asyncio
    async def test_watch_error_async_only_client(self):
        self.instrumentor.instrument_client(
            tracer_provider=self.tracer_provider, client=self.client
        )
        valkey_client = FakeAsyncValkey()
        await self._redis_pipeline_operations(valkey_client)

        spans = self.memory_exporter.get_finished_spans()

        # there should be 3 tests, we start watch operation and have 2 set operation on same key
        self.assertEqual(len(spans), 0)

        # now with the instrumented client we should get proper spans
        await self._redis_pipeline_operations(self.client)

        spans = self.memory_exporter.get_finished_spans()

        # there should be 3 tests, we start watch operation and have 2 set operation on same key
        self.assertEqual(len(spans), 3)

        self.assertEqual(spans[0].attributes.get("db.statement"), "WATCH ?")
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)
        self.assertEqual(spans[0].status.status_code, trace.StatusCode.UNSET)

        for span in spans[1:]:
            self.assertEqual(span.attributes.get("db.statement"), "SET ? ?")
            self.assertEqual(span.kind, SpanKind.CLIENT)
            self.assertEqual(span.status.status_code, trace.StatusCode.UNSET)
        ValkeyInstrumentor().uninstrument_client(self.client)

    @pytest.mark.asyncio
    async def test_request_response_hooks(self):
        request_attr = "my.request.attribute"
        response_attr = "my.response.attribute"

        def request_hook(span, conn, args, kwargs):
            if span and span.is_recording():
                span.set_attribute(request_attr, args[0])

        def response_hook(span, conn, args):
            if span and span.is_recording():
                span.set_attribute(response_attr, args)

        self.instrumentor.instrument(
            tracer_provider=self.tracer_provider,
            request_hook=request_hook,
            response_hook=response_hook,
        )
        await self.client.set("key", "value")

        spans = self.assert_span_count(1)

        span = spans[0]
        self.assertEqual(span.attributes.get(request_attr), "SET")
        self.assertEqual(span.attributes.get(response_attr), True)
        self.instrumentor.uninstrument()

    @pytest.mark.asyncio
    async def test_request_response_hooks_connection_only(self):
        request_attr = "my.request.attribute"
        response_attr = "my.response.attribute"

        def request_hook(span, conn, args, kwargs):
            if span and span.is_recording():
                span.set_attribute(request_attr, args[0])

        def response_hook(span, conn, args):
            if span and span.is_recording():
                span.set_attribute(response_attr, args)

        self.instrumentor.instrument_client(
            client=self.client,
            tracer_provider=self.tracer_provider,
            request_hook=request_hook,
            response_hook=response_hook,
        )
        await self.client.set("key", "value")

        spans = self.assert_span_count(1)

        span = spans[0]
        self.assertEqual(span.attributes.get(request_attr), "SET")
        self.assertEqual(span.attributes.get(response_attr), True)
        # fresh client should not record any spans
        fresh_client = FakeAsyncValkey()
        self.memory_exporter.clear()
        await fresh_client.set("key", "value")
        self.assert_span_count(0)
        self.instrumentor.uninstrument_client(self.client)
        # after un-instrumenting the query should not be recorder
        await self.client.set("key", "value")
        spans = self.assert_span_count(0)

    @pytest.mark.asyncio
    async def test_span_name_empty_pipeline(self):
        valkey_client = FakeAsyncValkey()
        self.instrumentor.instrument_client(
            client=valkey_client, tracer_provider=self.tracer_provider
        )
        async with valkey_client.pipeline() as pipe:
            await pipe.execute()

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0].name, "valkey")
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)
        self.assertEqual(spans[0].status.status_code, trace.StatusCode.UNSET)
        self.instrumentor.uninstrument_client(client=valkey_client)

    @pytest.mark.asyncio
    async def test_suppress_instrumentation_async_command(self):
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)
        valkey_client = FakeAsyncValkey()

        # Execute command with suppression
        with suppress_instrumentation():
            await valkey_client.get("key")

        # No spans should be created
        self.assert_span_count(0)

        # Verify that instrumentation works again after exiting the context
        await valkey_client.set("key", "value")
        self.assert_span_count(1)
        self.instrumentor.uninstrument()

    @pytest.mark.asyncio
    async def test_suppress_instrumentation_async_pipeline(self):
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)
        valkey_client = FakeAsyncValkey()

        # Execute pipeline with suppression
        with suppress_instrumentation():
            async with valkey_client.pipeline() as pipe:
                await pipe.set("key1", "value1")
                await pipe.set("key2", "value2")
                await pipe.get("key1")
                await pipe.execute()

        # No spans should be created
        self.assert_span_count(0)

        # Verify that instrumentation works again after exiting the context
        async with valkey_client.pipeline() as pipe:
            await pipe.set("key3", "value3")
            await pipe.execute()

        spans = self.assert_span_count(1)
        # Pipeline span could be "SET" or "valkey.pipeline" depending on implementation
        self.assertIn(spans[0].name, ["SET", "valkey.pipeline"])
        self.instrumentor.uninstrument()

    @pytest.mark.asyncio
    async def test_suppress_instrumentation_async_mixed(self):
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)
        valkey_client = FakeAsyncValkey()

        # Regular instrumented call
        await valkey_client.set("key1", "value1")
        self.assert_span_count(1)
        self.memory_exporter.clear()

        # Suppressed call
        with suppress_instrumentation():
            await valkey_client.set("key2", "value2")

        self.assert_span_count(0)

        # Another regular instrumented call
        await valkey_client.get("key1")
        self.assert_span_count(1)
        self.instrumentor.uninstrument()


class TestRedisInstance(TestBase):
    def assert_span_count(self, count: int):
        """
        Assert that the memory exporter has the expected number of spans.
        Returns the spans list if assertion passes
        """
        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), count)
        return spans

    def setUp(self):
        super().setUp()
        self.client = fakeredis.FakeStrictValkey()
        ValkeyInstrumentor().instrument_client(
            client=self.client, tracer_provider=self.tracer_provider
        )

    def tearDown(self):
        super().tearDown()
        ValkeyInstrumentor().uninstrument_client(self.client)

    def test_only_client_instrumented(self):
        valkey_client = valkey.Valkey()

        with mock.patch.object(valkey_client, "connection"):
            valkey_client.get("key")

        spans = self.assert_span_count(0)

        # now use the test client
        with mock.patch.object(self.client, "connection"):
            self.client.get("key")
        spans = self.assert_span_count(1)
        span = spans[0]
        self.assertEqual(span.name, "GET")
        self.assertEqual(span.kind, SpanKind.CLIENT)

    @staticmethod
    def redis_operations(client):
        with pytest.raises(WatchError):
            pipe = client.pipeline(transaction=True)
            pipe.watch("a")
            client.set("a", "bad")  # This will cause the WatchError
            pipe.multi()
            pipe.set("a", "1")
            pipe.execute()

    def test_watch_error_sync_only_client(self):
        valkey_client = fakeredis.FakeStrictValkey()

        self.redis_operations(valkey_client)

        self.assert_span_count(0)

        self.redis_operations(self.client)

        # there should be 3 tests, we start watch operation and have 2 set operation on same key
        spans = self.assert_span_count(3)

        self.assertEqual(spans[0].attributes.get("db.statement"), "WATCH ?")
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)
        self.assertEqual(spans[0].status.status_code, trace.StatusCode.UNSET)

        for span in spans[1:]:
            self.assertEqual(span.attributes.get("db.statement"), "SET ? ?")
            self.assertEqual(span.kind, SpanKind.CLIENT)
            self.assertEqual(span.status.status_code, trace.StatusCode.UNSET)
