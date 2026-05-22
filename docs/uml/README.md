# UML Diagrams

PlantUML source for Arachnite's architecture. Each `.puml` file renders to a
standalone diagram; view them together for a full picture.

| File                              | Diagram type    | Covers                                                         |
| --------------------------------- | --------------- | -------------------------------------------------------------- |
| `01_node_hierarchy.puml`          | Class           | `BaseNode` and the sense/instinct/decision/action subclasses   |
| `02_data_models.puml`             | Class           | `Signal`, `Context`, `Proposal`, `Result`, enums, supervisor signals |
| `03_runtime_composition.puml`     | Class           | `ArachniteRuntime` composition: bus, masters, supervisors, safety, shutdown, logging |
| `04_tick_sequence.puml`           | Sequence        | One `ArachniteRuntime.tick()`: sense → context → reflex → instinct → decide → act |
| `05_distributed.puml`             | Class / package | `MeshRuntime`, `AgentNode`, `DeploymentManifest`, transports, codecs |

## Render

PlantUML JAR (offline):

```bash
plantuml -tpng docs/uml/*.puml          # → PNG next to each .puml
plantuml -tsvg docs/uml/*.puml          # → SVG
```

VS Code: install the **PlantUML** extension and preview with `Alt+D`.

Online: paste contents into <https://www.plantuml.com/plantuml>.

## Conventions

- Master nodes own their child registries (composition, filled diamond).
- Reflex proposals (priority ≥ 200) bypass `DecisionMasterNode` by design.
- All inter-node communication flows through `SignalBus`; nodes never hold direct references.
- The co-location invariant — reflex instinct and its target action on the same `AgentNode` — is validated in `DeploymentManifest.validate()`.
