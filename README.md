# opentelemetry-python-contrib-external-valkey

[OpenTelemetry](https://opentelemetry.io/) instrumentation for the [Valkey Python Client](https://github.com/valkey-io/valkey-py).

## How do I install it?

`opentelemetry-python-contrib-external-valkey` is available on [PyPI](https://pypi.org/project/opentelemetry-python-contrib-external-valkey). You can install it with

```shell
pip install opentelemetry-python-contrib-external-valkey
```

or

```shell
uv add opentelemetry-python-contrib-external-valkey
```

or something equivalent.

## How do I use it?

For global instrumentation of the [Valkey Python client](https://github.com/valkey-io/valkey-py) you can do

```python
from opentelemetry.instrumentation.valkey import ValkeyInstrumentor

ValkeyInstrumentor().instrument()
```

For more detailed documentation please have a look at the [`opentelemetry-instrumentation-redis` documentation](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/redis/redis.html) and just replace `[rR]edis` with `[vV]alkey`. 

## Why is this needed?

`opentelemetry-instrumentation-redis` instruments the `redis` module and has multiple `isinstance` checks for classes from it. Since the [Valkey Python Client](https://github.com/valkey-io/valkey-py) is a fork of the [Redis Python Client](https://github.com/redis/redis-py), any instrumentation through `opentelemetry-instrumentation-redis` has no effect on the `valkey` module or any of its classes. `opentelemetry-python-contrib-external-valkey` fills this gap until Valkey is [officially supported](https://github.com/open-telemetry/opentelemetry-python-contrib/pull/3478) and gives Valkey users the same user experience for instrumentation as Redis users. 

## How does it work?

This repository is a fork of [opentelemetry-python-contrib](https://github.com/open-telemetry/opentelemetry-python-contrib) and specifically the [instrumentation/opentelemetry-instrumentation-redis](https://github.com/open-telemetry/opentelemetry-python-contrib/tree/main/instrumentation/opentelemetry-instrumentation-redis) folder. 95% and more of the changes in the [`src`](./src) and [`tests`](./tests) folders are just replacing the strings `redis` and `Redis` with `valkey` and `Valkey` respectively.
