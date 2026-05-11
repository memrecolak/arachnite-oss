# Advanced 6 — Configuration Injection

So far, every "tunable" in your nodes has been a hardcoded number. Threshold
> 40. Tick rate 5.0. API key in source code (you wouldn't, would you?).

Real agents need their settings to come from outside the code:

- Different environments (dev, staging, prod) need different values.
- Secrets shouldn't be checked into git.
- Operators want to tweak thresholds without rebuilding.

Arachnite has a small but capable config system: `NodeConfig`.

## NodeConfig — typed access to a dict

`NodeConfig` wraps a dictionary and gives you **typed accessors** with
defaults. You can also nest sub-sections and reference environment variables.

```python
from arachnite import NodeConfig

config = NodeConfig(
    data={
        "threshold": 85,
        "api_key": "${API_KEY}",      # env var interpolation
        "filters": {
            "enabled": True,
            "window_size": 5,
        },
    },
    node_id="ThermalSensor",
)

threshold = config.get_int("threshold", default=80)
api_key   = config.get_str("api_key")               # required, raises if missing
filters   = config.section("filters")
window    = filters.get_int("window_size", default=3)
enabled   = filters.get_bool("enabled", default=False)
```

The typed accessors are:

| Method | Returns |
|---|---|
| `get(key, default=...)` | Any type |
| `get_str(key, default=...)` | `str` |
| `get_int(key, default=...)` | `int` |
| `get_float(key, default=...)` | `float` |
| `get_bool(key, default=...)` | `bool` |
| `get_list(key, default=...)` | `list` |
| `section(key)` | A nested `NodeConfig` |

Without a default, missing keys raise `NodeConfigError`. That's a feature —
it forces you to declare what's required.

## Environment variable interpolation

Strings of the form `${VAR}` or `${VAR:-default}` are resolved at access
time:

```python
data = {
    "broker_host": "${MQTT_HOST:-localhost}",
    "api_key": "${ANTHROPIC_API_KEY}",
}
```

If `MQTT_HOST` isn't set, it falls back to `localhost`. If
`ANTHROPIC_API_KEY` isn't set, the access raises an error (no default).

This is how you keep secrets out of your code: put `${API_KEY}` in the
config file, and set `API_KEY` in your shell or your container environment.

## Wiring config into a node

The most common pattern is to take `config` as a constructor argument:

```python
from arachnite import BaseSenseNode, NodeConfig, Signal
import time


class ThermalSensor(BaseSenseNode):
    node_id = "ThermalSensor"
    signal_kind = "temperature"

    def __init__(self, bus, config: NodeConfig | None = None, **kwargs):
        super().__init__(bus=bus, **kwargs)
        self.config = config or NodeConfig(data={}, node_id=self.node_id)
        self.pin = self.config.get_int("pin", default=4)
        self.offset = self.config.get_float("offset", default=0.0)
        self.threshold = self.config.get_int("threshold", default=40)

    async def read(self) -> Signal:
        raw = await self._read_pin(self.pin)
        return Signal(
            source=self.node_id,
            kind=self.signal_kind,
            value=raw + self.offset,
            confidence=1.0,
            timestamp=time.monotonic(),
        )

    async def _read_pin(self, pin: int) -> float:
        return 42.0  # pretend hardware
```

When you build the node, you pass it a `NodeConfig`:

```python
sensor = ThermalSensor(
    bus=bus,
    config=NodeConfig(
        data={"pin": 17, "offset": -1.5, "threshold": 38},
        node_id="ThermalSensor",
    ),
)
```

That's the whole API. Three lines to declare what's tunable, one line to
provide values.

## Where the config comes from

In a small program, you build the `NodeConfig` in `main()` and pass it in.
In a real deployment, the config comes from a **deployment manifest** — a
YAML file that describes your whole agent. We'll cover manifests in lesson
7 (Going Distributed); for now just know that this:

```yaml
agents:
  - id: "thermal-edge"
    nodes:
      sense:
        - kind: "myapp.sensors.ThermalSensor"
          config:
            pin: 17
            offset: -1.5
            threshold: 38
```

becomes a `NodeConfig` automatically when the agent starts. You write the
node once and configure it many ways.

## Tips

1. **Always provide defaults** for fields that *can* have defaults. Make
   `get_int("threshold", default=40)` your habit, not `get_int("threshold")`.
2. **Required fields go without a default.** That way the agent fails fast
   on startup if config is missing, rather than crashing on tick 1000.
3. **Group related settings into sections.** `filters.window_size` is
   easier to read than `filter_window_size`.
4. **Never hardcode secrets** — use `${VAR}` interpolation.
5. **Don't put behaviour in config.** Config is for *values*. If you find
   yourself adding `mode: aggressive` and writing `if mode == "aggressive"`
   in the node, write two different instinct classes instead.

## What's next?

You can now build a single agent that's flexible, observable, supervised,
and clever. Next: how to take that agent and **split it across multiple
machines**.

[← Logging and Observability](05_logging_and_observability.md) | [Next: Going Distributed →](07_going_distributed.md)
