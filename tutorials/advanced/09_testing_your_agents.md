# Advanced 9 — Testing Your Agents

There's a temptation, with reactive systems, to "just run them and watch the
output". Don't. Reactive agents have a lot of moving parts and a lot of
silent failure modes — a sensor returning the wrong unit, an instinct that
never fires because of a typo, an action that swallows its own errors. The
only way to keep all of that working as your agent grows is **automated
tests**.

The good news: every Arachnite node has a tiny, well-defined contract,
which makes them the easiest kind of code in the world to unit-test.

## What to test

For each node type, there's one obvious thing to assert:

| Node type | What to test |
|---|---|
| `BaseSenseNode` | `read()` returns the right `Signal` |
| `BaseInstinctNode` | `evaluate(ctx)` returns the right `Proposal` (or `None`) for given signals |
| `BaseActionNode` | `execute(proposal)` returns a successful `Result` and performs the side effect |
| `BaseDecisionNode` | `decide(proposals)` picks the right one |
| `MultiStepActionNode` | The right steps run, in the right order, with the right rollback behaviour |

You don't have to start the runtime to test any of these. You build the
node, build a fake input, call the method directly, and assert on the
output. Tests run in milliseconds.

## The setup

Arachnite tests use **pytest** and **pytest-asyncio**. Install them:

```bash
pip install pytest pytest-asyncio
```

A test file looks like this:

```python
# tests/test_my_sensor.py
import pytest
import time

from arachnite import SignalBus, Signal
from myapp.sensors import ThermalSensor


@pytest.mark.asyncio
async def test_thermal_sensor_returns_signal():
    bus = SignalBus()
    sensor = ThermalSensor(bus=bus)
    sig = await sensor.read()
    assert isinstance(sig, Signal)
    assert sig.kind == "temperature"
    assert sig.confidence == 1.0
```

The `@pytest.mark.asyncio` decorator tells pytest "this test is async, run
it inside an event loop." That's the only ceremony you need.

## Testing an instinct

To test an instinct, you build a fake `Context` with the signals you want,
call `evaluate()`, and assert on the proposal it returns:

```python
import pytest
import time
from collections import deque

from arachnite import SignalBus, Signal, Context
from myapp.instincts import HotInstinct


def make_signal(kind: str, value: float) -> Signal:
    return Signal(
        source="test",
        kind=kind,
        value=value,
        confidence=1.0,
        timestamp=time.monotonic(),
    )


def make_context(signals: list[Signal]) -> Context:
    return Context(
        tick=1,
        signals=signals,
        history=deque(),
        state={},
        last_result=None,
        last_results=[],
        action_state=None,
        action_states=[],
    )


class TestHotInstinct:
    @pytest.mark.asyncio
    async def test_fires_when_temperature_above_threshold(self):
        instinct = HotInstinct(bus=SignalBus())
        ctx = make_context([make_signal("temperature", 45.0)])
        proposal = await instinct.evaluate(ctx)
        assert proposal is not None
        assert proposal.action_id == "CoolDown"
        assert proposal.priority == 80

    @pytest.mark.asyncio
    async def test_does_not_fire_when_below_threshold(self):
        instinct = HotInstinct(bus=SignalBus())
        ctx = make_context([make_signal("temperature", 20.0)])
        proposal = await instinct.evaluate(ctx)
        assert proposal is None

    @pytest.mark.asyncio
    async def test_ignores_unrelated_signals(self):
        instinct = HotInstinct(bus=SignalBus())
        ctx = make_context([make_signal("humidity", 99.0)])
        proposal = await instinct.evaluate(ctx)
        assert proposal is None
```

Three tests, each one targeting a single behaviour. This is the **three
tests per instinct** rule of thumb: positive case, negative case, irrelevant
input.

## Testing an action

Actions are even easier — build a fake proposal, call `execute()`, assert
on the `Result`:

```python
import pytest
from arachnite import SignalBus, Proposal
from myapp.actions import CoolDown


def make_proposal(action_id: str, **kwargs) -> Proposal:
    return Proposal(
        instinct_id="test",
        action_id=action_id,
        priority=80,
        urgency=0.5,
        parameters=kwargs,
    )


class TestCoolDown:
    @pytest.mark.asyncio
    async def test_returns_success(self):
        action = CoolDown(bus=SignalBus())
        result = await action.execute(make_proposal("CoolDown"))
        assert result.success is True

    @pytest.mark.asyncio
    async def test_passes_parameters_through(self):
        action = CoolDown(bus=SignalBus())
        result = await action.execute(
            make_proposal("CoolDown", target_temp=25.0)
        )
        assert result.success is True
```

If your action talks to hardware or a network service, **mock that
boundary** — don't actually talk to the hardware in a test:

```python
from unittest.mock import AsyncMock

class TestCoolDownWithMock:
    @pytest.mark.asyncio
    async def test_calls_fan_api(self, monkeypatch):
        action = CoolDown(bus=SignalBus())
        action._fan_client = AsyncMock()
        await action.execute(make_proposal("CoolDown"))
        action._fan_client.turn_on.assert_called_once()
```

## Helpers worth writing once

You'll write `make_signal()`, `make_context()`, and `make_proposal()` over
and over. Put them in a `conftest.py` file at the top of your `tests/`
folder, and pytest will make them available everywhere automatically:

```python
# tests/conftest.py
import time
from collections import deque
import pytest
from arachnite import Signal, Context, Proposal


@pytest.fixture
def make_signal():
    def _make(kind: str, value, source: str = "test", confidence: float = 1.0) -> Signal:
        return Signal(
            source=source, kind=kind, value=value,
            confidence=confidence, timestamp=time.monotonic(),
        )
    return _make


@pytest.fixture
def make_context():
    def _make(signals=None, state=None, tick: int = 1) -> Context:
        return Context(
            tick=tick,
            signals=signals or [],
            history=deque(),
            state=state or {},
            last_result=None,
            last_results=[],
            action_state=None,
            action_states=[],
        )
    return _make
```

Now your tests use them as parameters:

```python
@pytest.mark.asyncio
async def test_fires_on_hot(make_signal, make_context):
    instinct = HotInstinct(bus=SignalBus())
    ctx = make_context(signals=[make_signal("temperature", 45)])
    assert await instinct.evaluate(ctx) is not None
```

Cleaner, less repetitive, and mistakes in the helpers get fixed in one
place.

## Integration tests — running a real tick or two

Unit tests cover individual nodes. Sometimes you also want to make sure the
whole pipeline ties together — sensor → instinct → decision → action. For
those, build a real runtime, run it for a few ticks, and assert on what
happened:

```python
import pytest
import asyncio

from arachnite import (
    SignalBus, ContextNode, ArachniteRuntime,
    SenseMasterNode, InstinctMasterNode,
    DecisionMasterNode, GreedyDecisionNode,
    ActionMasterNode,
)
from myapp.sensors import ThermalSensor
from myapp.instincts import HotInstinct
from myapp.actions import RecordingCoolDown  # action that records calls


@pytest.mark.asyncio
async def test_hot_temperature_triggers_cooldown():
    bus = SignalBus()
    sense_master = SenseMasterNode(bus=bus)
    instinct_master = InstinctMasterNode(bus=bus)
    decision_master = DecisionMasterNode(
        bus=bus, strategy=GreedyDecisionNode(bus=bus)
    )
    action_master = ActionMasterNode(bus=bus)

    cooldown = RecordingCoolDown(bus=bus)

    sense_master.register(ThermalSensor(bus=bus))
    instinct_master.register(HotInstinct(bus=bus))
    action_master.register(cooldown)

    rt = ArachniteRuntime(
        sense_master=sense_master,
        context=ContextNode(),
        instinct_master=instinct_master,
        decision_master=decision_master,
        action_master=action_master,
        bus=bus,
        tick_rate_hz=10.0,
    )

    await rt.start()
    await asyncio.sleep(0.5)  # ~5 ticks
    await rt.stop()

    assert cooldown.call_count > 0
```

`RecordingCoolDown` is a test-only action that increments a counter every
time it's called. Patterns like this are how you assert on emergent
behaviour without scraping logs.

## Tips

1. **One assertion per test, where possible.** Easier to read, easier to
   debug, easier to extend.
2. **Fakes > mocks for sensors.** A small "fake sensor that returns the
   value I gave it" is more readable than mocking `read()`.
3. **Keep tests fast.** Set tick rates as high as possible (50–100 Hz),
   sleep as little as possible. Whole test suites should run in seconds.
4. **Test the unhappy path.** What happens when the sensor returns 0?
   `None`? A negative number? An exception? Each one is a test.
5. **Don't test the framework.** Focus on *your* nodes. Trust that
   `GreedyDecisionNode` picks the highest priority — it's already tested
   in Arachnite's own test suite.

## Wrapping up

You now have a complete picture of Arachnite, from the simplest hello-agent
to a distributed multi-machine deployment with LLM-backed decisions and
auto-restarting nodes. **Most agents won't need most of this.** Pick the
features you actually need and ignore the rest.

The single best thing you can do from here is **build something real**.
Pick a problem you actually have — a smart desk light, a personal log
analyzer, a tiny game enemy — and build it. Reading more docs has
diminishing returns. Writing your own code does not.

Good luck.

[← LLM Instincts](08_llm_instincts.md) | [Back to advanced index](README.md) | [Back to beginner course](../README.md)
