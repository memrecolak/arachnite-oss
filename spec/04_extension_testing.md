<!-- Arachnite SPEC §8–§9 -->

# __8\. Extension Guide__

## __8\.1 Custom SenseNode__

- Extend BaseSenseNode\.
- Set signal\_kind and poll\_interval\_s\.
- Implement read\(\)\. Wrap blocking I/O in asyncio\.to\_thread\(\)\.
- Override on\_error\(\) to handle hardware failures gracefully\.

## __8\.2 Custom InstinctNode__

- Extend BaseInstinctNode\.
- Choose a priority: 100\+ safety, 50\-99 goal\-driven, 1\-49 exploratory\.
- Implement evaluate\(\)\. Keep it fast and stateless where possible\.
- Use ctx\.state for any persistent reasoning state\.

## __8\.3 Custom ReflexInstinctNode__

- Extend BaseReflexInstinctNode instead of BaseInstinctNode\.
- Reserve for genuinely time\-critical responses only\. If the situation can wait one tick, use a normal InstinctNode\.
- Implement evaluate\(\)\. It must be synchronous in intent — no awaiting slow resources\.
- Set priority above 100 to distinguish from normal instincts\. Use 200\+ for the most critical reflexes\.
- Subscribe to 'supervisor' signals to build fault\-reactive reflexes\.
- Set preemption \(ReflexPreemption\.PREEMPT, the default, or WAIT\_FOR\_BLOCK\) to decide what the reflex does to a running action that holds the actuators it needs \(§17\.12\)\.
- Set trigger\_on\_signals so the reflex can run in the event\-driven lane \(reflex\_mode='event', §7\.2\.3\)\.

## __8\.4 Custom DecisionNode__

- Extend BaseDecisionNode\.
- Implement decide\(\)\. The proposals list is pre\-sorted by priority descending\.
- Returning None is valid: it means the agent idles this tick\.

## __8\.5 Custom ActionNode__

- Extend BaseActionNode\. Set node\_id to the value InstinctNodes will use in Proposal\.action\_id\.
- Implement execute\(\)\. Always return a Result, even on failure\. Make it cancellation\-safe: a reflex preemption or emergency stop arrives as asyncio\.CancelledError at an await\.
- Set timeout\_s and max\_retries to protect against hardware hangs\.
- Declare resources — every actuator the action commands — so the framework can enforce actuator exclusivity\.
- Override on\_preempted\(\) with a forward safe\-stop \(brake, keep the grip\) that finishes within safe\_stop\_timeout\_s\.

## __8\.6 Custom MultiStepActionNode__

- Extend MultiStepActionNode instead of BaseActionNode when the action has multiple ordered phases\.
- Implement steps\(\) returning the ordered list of ActionSteps\. Mark steps interruptible=False only when stopping mid\-step would leave hardware in an unsafe state\.
- Implement execute\_step\(\) with a match/case on step\.name\. Always return a StepResult — never raise\.
- Set interrupt\_policy to match the action’s safety requirements: ROLLBACK for physical state that must be undone, NEVER for sequences that must always complete once started\.
- Provide rollback callables on non\-interruptible steps wherever possible\. A step without a rollback in a ROLLBACK policy action is a design smell — document why it’s safe to leave it unreversed\.
- The longest a mandatory block can hold off a deliberative interrupt or a WAIT\_FOR\_BLOCK reflex is the sum of timeout\_s over the block \(plus safe\_stop\_timeout\_s for the reflex\); a PREEMPT reflex waits only for safe\_stop\_timeout\_s \(§17\.12\)\. Make this explicit in the class docstring so operators can reason about worst\-case response times\.
- Set rollback\_on\_preempt=True only if undoing completed steps during a hazard is safe\.

## __8\.7 Configuring a NodeSupervisor__

- Each master node creates its supervisor automatically\. Configure it via the supervisor property\.
- Set restart\_policy to RestartPolicy\.NEVER for nodes where a restart could cause unsafe hardware state\.
- Set max\_restarts=0 with RestartPolicy\.NEVER to go directly to DEAD on first fault\.
- Subscribe a ReflexInstinctNode to 'supervisor' signals to trigger safe\-mode actions on node death\.

## __8\.8 Priority Convention__

__Priority Range__

__Instinct Class__

__200\+__

Reflex \(ReflexInstinctNode only\)\. Critical safety bypass\. Emergency stop, sensor fault response\.

__100 – 199__

Normal instinct: safety / survival\. Overheating, collision risk, power loss\.

__50 – 99__

Normal instinct: goal\-directed\. Move toward target, complete task, respond to user\.

__1 – 49__

Normal instinct: exploratory / maintenance\. Wander, self\-test, conserve energy\.

__0__

Reserved\. A proposal with priority 0 is treated as inactive\.

## __8\.9 Hardware Integration__

Arachnite does not impose a hardware abstraction layer\. `BaseSenseNode\.read\(\)` and `BaseActionNode\.execute\(\)` are the abstraction boundary — each node owns its driver code and wraps blocking I/O in `asyncio\.to\_thread\(\)`\.

__Recommended pattern:__

1\. __Declare hardware config in the manifest__ — pin numbers, device addresses, backend library names go in the node's `config` section\. The node reads them via `self\.config\.get\(\)`\.

```yaml
nodes:
  sense:
    \- kind: myapp\.nodes\.ProximitySense
      config:
        gpio\_pin: 17
        backend: libgpiod
```

2\. __Initialise hardware in `setup\(\)`__ — import the platform\-specific driver lazily and open the device\. This keeps the driver import off the critical path and allows the node to run with a stub driver in tests\.

```python
class ProximitySense\(BaseSenseNode\):
    async def setup\(self\) \-> None:
        pin = self\.config\.get\_int\("gpio\_pin", 17\)
        backend = self\.config\.get\_str\("backend", "RPi\.GPIO"\)
        self\.\_driver = await asyncio\.to\_thread\(\_open\_gpio, pin, backend\)

    async def read\(self\) \-> Signal:
        value = await asyncio\.to\_thread\(self\.\_driver\.read\)
        return Signal\(source=self\.node\_id, kind=self\.signal\_kind,
                      value=value, confidence=1\.0, timestamp=time\.monotonic\(\)\)
```

3\. __Clean up in `teardown\(\)`__ — release pins, close connections, stop streams\.

4\. __Use `Permission` for capability control__ — if a node requires network, GPIO, or GPU access, declare it via the `permissions` class attribute \(§11\.3\.1\)\.

5\. __Test with stubs, not mocks__ — inject a fake driver object that returns deterministic values\. No framework changes needed: just pass different config to the node constructor in tests\.

__Why no built\-in HAL:__ Hardware APIs vary too widely across platforms \(RPi\.GPIO vs libgpiod vs Jetson\.GPIO, picamera2 vs OpenCV vs V4L2\)\. A thin wrapper adds indirection without solving the real complexity\. The config\-injection pattern gives nodes full control over their drivers while keeping the framework hardware\-agnostic\.

# __9\. Testing__

## __9\.1 Unit Testing Nodes__

Every node is testable in isolation\. The SignalBus and ContextNode can be instantiated without the full runtime\.

async def test\_overheat\_instinct\(\):

    bus = SignalBus\(\)

    ctx = Context\(

        tick=1,

        signals=\[Signal\(source='temp', kind='thermal', value=95\.0,

                        confidence=1\.0, timestamp=0\.0\)\],

        history=deque\(\),

        state=\{\},

        last\_result=None,

        timestamp=0\.0,

    \)

    node = OverheatInstinct\(bus=bus, priority=100\)

    proposal = await node\.evaluate\(ctx\)

    assert proposal is not None

    assert proposal\.action\_id == 'SetTemperatureActionNode'

## __9\.2 Testing Reflex Nodes__

Reflex nodes can be tested identically to normal instinct nodes\. The key difference is verifying that the runtime launches them before the decision step\. Because ``tick\(\)`` launches actions without waiting for them, wait for the action and run one more tick before reading its Result from the context\.

async def test\_emergency\_stop\_reflex\_bypasses\_decision\(\):

    runtime = build\_test\_runtime\(\)   \# a stub proximity sense node reads 0\.05

    await runtime\.start\(\)

    await runtime\.pause\(\)            \# drive ticks manually

    await runtime\.wait\_idle\(timeout=1\.0\)   \# settle background\-loop ticks

    await runtime\.tick\(\)             \# reflex fires; action launched

    assert await runtime\.wait\_idle\(timeout=1\.0\)

    await runtime\.tick\(\)             \# Result surfaces in this tick's ctx

    ctx = runtime\.context\.snapshot\(\)

    assert ctx\.last\_result\.action\_id == 'EmergencyStopActionNode'

    await runtime\.stop\(\)

## __9\.3 Testing the NodeSupervisor__

Supervisors can be tested by injecting a faulting node and asserting the state transition and signal emission\.

async def test\_supervisor\_emits\_signal\_on\_fault\(\):

    bus = SignalBus\(\)

    received: list\[SupervisorSignal\] = \[\]

    bus\.subscribe\('supervisor', lambda s: received\.append\(s\)\)

    supervisor = NodeSupervisor\(bus=bus, restart\_policy=RestartPolicy\.NEVER\)

    node = FaultyNode\(bus=bus\)   \# raises on setup\(\)

    supervisor\.track\(node\)

    await node\.setup\(\)   \# triggers fault

    await asyncio\.sleep\(0\.05\)   \# let bus dispatch

    assert supervisor\.state\_of\(node\.node\_id\) == NodeState\.DEAD

    assert len\(received\) == 1

    assert received\[0\]\.current\_state == NodeState\.DEAD

## __9\.4 Integration Testing with Manual Tick__

The runtime exposes a tick\(\) method for step\-by\-step testing without a real clock loop\. tick\(\) launches actions as tasks and returns without waiting for them; use wait\_idle\(\) and one more tick to observe their Results\.

async def test\_full\_pipeline\(\):

    runtime = build\_test\_runtime\(\)   \# a stub thermal sense node reads 90\.0

    await runtime\.start\(\)

    await runtime\.pause\(\)

    await runtime\.wait\_idle\(timeout=1\.0\)

    await runtime\.tick\(\)             \# instinct proposes; action launched

    assert await runtime\.wait\_idle\(timeout=1\.0\)

    await runtime\.tick\(\)             \# Result surfaces in ctx

    result = runtime\.context\.snapshot\(\)\.last\_result

    assert result\.success

    assert runtime\.health\.system\_healthy\(\)

    await runtime\.stop\(\)

## __9\.5 Testing MultiStepActionNode__

MultiStepActionNode can be tested step\-by\-step using execute\_step\(\) directly, or end\-to\-end via the runtime\. Interrupt behaviour is tested by injecting a competing proposal mid\-execution\. When execute\(\) is called directly, it resets the interrupt flag on entry, so the interrupt must be requested after execute\(\) has started\.

async def test\_interrupt\_stops\_at\_safe\_point\(\):

    node = PickAndPlaceActionNode\(bus=SignalBus\(\)\)

    proposal = Proposal\(instinct\_id='nav', action\_id=node\.node\_id,

                        priority=50, urgency=0\.7, parameters=\{

                            'object\_position': \(1\.0, 0\.5, 0\.0\),

                        \}\)

    \# Start execution in a background task

    task = asyncio\.create\_task\(node\.execute\(proposal\)\)

    await asyncio\.sleep\(0\.1\)   \# let move\_to\_object begin

    \# Inject a higher\-priority deliberative interrupt

    interrupt = InterruptRequest\(

        new\_proposal=Proposal\(instinct\_id='avoid', priority=120, \.\.\.\),

        requesting\_instinct\_id='ObstacleAvoidInstinct',

    \)

    node\.request\_interrupt\(interrupt\)

    result = await task

    \# The flag is held through the mandatory block \(lower, close, raise\)

    \# and honoured at the first interruptible boundary after it; ROLLBACK

    \# then undoes the block in reverse order\.

    assert result\.interrupted

    assert result\.stopped\_at\_step == 'move\_to\_target'

    assert result\.rolled\_back

async def test\_mandatory\_block\_not\_interrupted\(\):

    node = PickAndPlaceActionNode\(bus=SignalBus\(\)\)

    \# Simulate interrupt arriving during close\_gripper \(non\-interruptible\)

    \# Verify action completes raise\_gripper before yielding

    \# \.\.\.

Reflex preemption is tested through ActionMasterNode: launch the action, then call launch\_reflex\(\) with a proposal whose action shares a resource, and assert the owner's Result has preempted=True, that on\_preempted\(\) ran, and that the reflex action started only afterwards\.

