"""Unit tests for the stateless tool codec (encode + tolerant multi-dialect decode)."""

from __future__ import annotations

from arachnite.toolcodec import parse_tool_calls, render_tool_schemas

# ── decode: parse_tool_calls, one test per dialect ────────────────────────────


def test_parse_tool_call_tag() -> None:
    calls = parse_tool_calls('<tool_call>{"name": "f", "arguments": {"x": 1}}</tool_call>')
    assert len(calls) == 1
    assert calls[0].name == "f"
    assert calls[0].arguments == {"x": 1}
    assert calls[0].source == "tag"


def test_parse_multiple_tags() -> None:
    text = ('<tool_call>{"name": "a", "arguments": {}}</tool_call>'
            '<tool_call>{"name": "b", "arguments": {}}</tool_call>')
    assert [c.name for c in parse_tool_calls(text)] == ["a", "b"]


def test_parse_unclosed_tag_with_array() -> None:
    # Granite/Falcon: an unclosed <tool_call> wrapping a JSON array.
    calls = parse_tool_calls('<tool_call>[{"name": "a", "arguments": {}}]')
    assert [c.name for c in calls] == ["a"]


def test_parse_fn_paren() -> None:
    calls = parse_tool_calls('search({"q": "cats"})')
    assert calls and calls[0].name == "search"
    assert calls[0].arguments == {"q": "cats"} and calls[0].source == "fn_paren"


def test_parse_bare_json() -> None:
    calls = parse_tool_calls('{"name": "f", "arguments": {"x": 1}}')
    assert calls and calls[0].source == "bare_json" and calls[0].arguments == {"x": 1}


def test_parse_json_array() -> None:
    calls = parse_tool_calls('[{"name": "a", "arguments": {}}, {"name": "b", "arguments": {}}]')
    assert [c.name for c in calls] == ["a", "b"]


def test_parse_parameters_alias_and_code_fence() -> None:
    # `parameters` alias, wrapped in a ```json fence.
    text = '```json\n{"name": "f", "parameters": {"x": 1}}\n```'
    calls = parse_tool_calls(text)
    assert calls and calls[0].arguments == {"x": 1}


def test_parse_double_encoded_arguments() -> None:
    calls = parse_tool_calls('{"name": "f", "arguments": "{\\"x\\": 1}"}')
    assert calls and calls[0].arguments == {"x": 1}


def test_parse_angle_wrapped() -> None:
    calls = parse_tool_calls('<{"name": "f", "arguments": {"x": 1}}>')
    assert calls and calls[0].source == "angle_json"


def test_parse_embedded_in_wrapper_tag() -> None:
    calls = parse_tool_calls('<function-call>{"name": "f", "arguments": {"x": 1}}</function-call>')
    assert calls and calls[0].source == "embedded_json"


def test_parse_prose_yields_nothing() -> None:
    assert parse_tool_calls("I think we should create a calculator module.") == []


def test_parse_empty_and_none() -> None:
    assert parse_tool_calls("") == []
    assert parse_tool_calls(None) == []


def test_parse_nested_braces_balanced() -> None:
    calls = parse_tool_calls('{"name": "f", "arguments": {"cfg": {"a": {"b": 1}}}}')
    assert calls and calls[0].arguments == {"cfg": {"a": {"b": 1}}}


# ── encode: render_tool_schemas ───────────────────────────────────────────────

_TOOLS = [
    {"type": "function", "function": {
        "name": "write_file",
        "description": "Create or overwrite a file.",
        "parameters": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "workspace-relative path"},
                "content": {"type": "string"},
            },
            "required": ["path", "content"],
        },
    }},
]


def test_render_lists_tool_and_params() -> None:
    text = render_tool_schemas(_TOOLS)
    assert "write_file" in text
    assert "Create or overwrite a file." in text
    assert "`path` (string, required)" in text
    assert "`content` (string, required)" in text
    assert "workspace-relative path" in text


def test_render_marks_optional_params() -> None:
    tools = [{"function": {"name": "f", "parameters": {
        "properties": {"opt": {"type": "integer"}}}}}]
    assert "`opt` (integer, optional)" in render_tool_schemas(tools)


def test_render_empty_tools_is_empty_string() -> None:
    assert render_tool_schemas([]) == ""


def test_render_accepts_bare_schema() -> None:
    assert "`grep`" in render_tool_schemas([{"name": "grep", "parameters": {"properties": {}}}])


def test_public_api_exported_from_root() -> None:
    import arachnite
    assert "parse_tool_calls" in arachnite.__all__
    assert "render_tool_schemas" in arachnite.__all__
    assert "ParsedToolCall" in arachnite.__all__
