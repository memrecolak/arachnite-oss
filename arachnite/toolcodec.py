"""
arachnite.toolcodec
~~~~~~~~~~~~~~~~~~~~~
A **stateless tool codec** for open-weight LLM tool use — two pure functions with
no framework, I/O, threads, or model dependency (cheap enough for a Raspberry Pi):

  * :func:`render_tool_schemas` — *encode*: render tool schemas into prompt text.
    Many open-weight chat templates silently ignore the provider's native ``tools=``
    parameter, so a model that is never shown a tool section answers in prose —
    indistinguishable from being incapable. Folding the schemas into the prompt as
    text makes the surface visible regardless of the template.
  * :func:`parse_tool_calls` — *decode*: recover tool calls a model emitted as free
    **text** across the dialects seen in the wild, when the provider returned no
    structured ``tool_calls``.

This is deliberately **separate from** :class:`~arachnite.llm_provider.LLMProvider`,
whose ``complete()`` is single-shot and reads only the provider's structured
tool-call field. The codec is what a caller composes *around* a provider (or a
multi-turn loop of its own) to make open-weight models drive tools reliably; it does
not change the provider contract.

Parser lifted and generalised from the format-bias study (Ariadne's original
``ToolCallChatBase._parse_tool_call_tags``); dialects, in priority order:

  1. ``<tool_call>{"name": …, "arguments": {…}}</tool_call>`` (closed or unclosed,
     single object or a JSON array)                              -> source "tag"
  2. ``function_name({…})`` (optional leading slash)             -> source "fn_paren"
  3. bare ``{"name": …, "arguments": {…}}`` JSON, or a JSON array of them
                                                                 -> source "bare_json"
  4. angle-wrapped ``<{…}>`` with an inconsistent trailing close -> source "angle_json"
  5. (last resort) a ``{"name": …}`` object embedded in an arbitrary wrapper tag
     (``<function-call>{…}</function-call>``, ``<json>{…}</json>``) -> "embedded_json"

Permissive throughout: a malformed candidate falls through to the next dialect
rather than raising.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Literal

__all__ = ["ParsedToolCall", "ToolCallSource", "parse_tool_calls", "render_tool_schemas"]

ToolCallSource = Literal["tag", "fn_paren", "bare_json", "angle_json", "embedded_json"]

_TAG_RE = re.compile(r"<tool_call>(.*?)</tool_call>", re.DOTALL)
_FN_PAREN_RE = re.compile(r"^/?(\w+)\s*\(\s*(\{.*\})\s*\)\s*$", re.DOTALL)
#: A whole-content markdown code fence, e.g. ```json\n{...}\n``` — some models wrap
#: their tool-call JSON in one, which would otherwise be missed.
_FENCE_RE = re.compile(r"^```[a-zA-Z0-9_]*\s*\n(.*?)\n?```$", re.DOTALL)
#: An opening angle-bracket tag token, e.g. ``<function-call>`` or ``<|python_tag|>``.
#: Dialect 5 only fires when such a wrapper precedes the embedded object, so prose
#: that merely quotes — or narrates an example of — a tool-call JSON is not mistaken
#: for a real invocation. Requires a non-space char right after ``<`` so a prose
#: ``x < 5`` comparison never matches.
_TAG_TOKEN_RE = re.compile(r"<[^<>\s][^<>]{0,60}>")


@dataclass(frozen=True)
class ParsedToolCall:
    """A single tool invocation recovered from assistant text, tagged with the
    ``source`` dialect that produced it (useful for diagnosing which models never
    emit structured calls)."""

    name: str
    arguments: dict[str, Any]
    source: ToolCallSource


# ── encode: schemas -> prompt text ────────────────────────────────────────────


def render_tool_schemas(tools: list[dict[str, Any]]) -> str:
    """Render OpenAI-form tool schemas as a readable prompt section.

    ``tools`` are function-calling dicts (``{"function": {"name", "description",
    "parameters"}}``; a bare ``{"name", …}`` is accepted too). Returns an empty
    string when there are no tools, so a caller can append it unconditionally.
    """
    if not tools:
        return ""
    out = [
        "\n\n## Available tools",
        "You have these tools. To call one, emit a JSON object "
        '`{"name": "<tool>", "arguments": {<args>}}` (your native tool-call format '
        "is also accepted) — one call per turn, and wait for its result before the "
        "next.",
        "",
    ]
    for tool in tools:
        fn = tool.get("function", tool)
        name = fn.get("name", "")
        desc = fn.get("description", "")
        out.append(f"- `{name}` — {desc}" if desc else f"- `{name}`")
        params = fn.get("parameters") or {}
        props = params.get("properties") or {}
        required = set(params.get("required") or [])
        for pname, pschema in props.items():
            out.append(_render_param(pname, pschema, required))
    return "\n".join(out) + "\n"


def _render_param(pname: str, pschema: Any, required: set[str]) -> str:
    schema = pschema if isinstance(pschema, dict) else {}
    ptype = schema.get("type", "any")
    req = "required" if pname in required else "optional"
    pdesc = schema.get("description", "")
    tail = f" — {pdesc}" if pdesc else ""
    return f"    - `{pname}` ({ptype}, {req}){tail}"


# ── decode: text -> tool calls ────────────────────────────────────────────────


def parse_tool_calls(content: str | None) -> list[ParsedToolCall]:
    """Recover tool calls embedded in assistant *text* content.

    Tries the dialects in priority order and returns as soon as one yields at least
    one call (a model emits one dialect, not a mix). Returns ``[]`` when nothing
    parses. Plain prose yields nothing, and prose that merely quotes — or narrates
    an example of — a tool-call JSON is not mistaken for a call: the last-resort
    embedded-object dialect fires only when a wrapper tag precedes the object.
    """
    if not content:
        return []

    # Dialect 1: <tool_call> tags — closed or unclosed, a single object OR a JSON
    # array (Qwen/Granite/Falcon).
    blobs = _TAG_RE.findall(content)
    if not blobs and "<tool_call>" in content:
        blobs = [content.split("<tool_call>", 1)[1]]
    tag_calls: list[ParsedToolCall] = []
    for blob in blobs:
        tag_calls.extend(_calls_from_blob(blob, source="tag"))
    if tag_calls:
        return tag_calls

    # Unwrap a surrounding ```json … ``` fence so the JSON dialects below see the
    # bare object (the tag dialect above ran on the raw content first).
    stripped = _strip_code_fence(content.strip())

    # Dialect 2: function_name({...}).
    m = _FN_PAREN_RE.match(stripped)
    if m:
        args = _loads(m.group(2))
        if m.group(1) and isinstance(args, dict):
            return [ParsedToolCall(name=m.group(1), arguments=args, source="fn_paren")]

    # Dialect 3: bare {"name": ..., "arguments": {...}} JSON.
    if stripped.startswith("{") and '"name"' in stripped:
        call = _call_from_name_args(stripped, source="bare_json")
        if call is not None:
            return [call]

    # Dialect 3b: a JSON array of tool calls [{"name": ...}, ...] (Mistral).
    if stripped.startswith("[") and '"name"' in stripped:
        arr = _loads(stripped)
        if isinstance(arr, list):
            calls = [
                c for item in arr if isinstance(item, dict)
                for c in (_call_from_dict(item, source="bare_json"),) if c is not None
            ]
            if calls:
                return calls

    # Dialect 4: angle-bracket-wrapped object <{"name":..,"arguments":..}> (Qwen-Coder)
    # with an inconsistent trailing close. Extract the brace-balanced object.
    if stripped.startswith("<"):
        inner = stripped[1:].lstrip()
        if inner.startswith("{"):
            obj = _first_json_object(inner)
            if obj is not None:
                c = _call_from_name_args(obj, source="angle_json")
                if c is not None:
                    return [c]

    # Dialect 5 (last-resort): a {"name":..} object recovered from content the
    # earlier dialects could not parse whole. Two legitimate shapes: the object is
    # at the START (a valid call followed by trailing prose — d3 rejected it only
    # because json.loads choked on the trailing junk), or it is wrapped in a NON-
    # <tool_call> tag (``<function-call>{…}</function-call>``, ``<|python_tag|>{…}``).
    # Fire ONLY in those two cases, so an object sitting AFTER prose — text that
    # merely quotes a JSON object or *narrates* an example call — is not mistaken
    # for an invocation (the false positives this gate exists to reject).
    brace = content.find("{")
    if brace >= 0:
        prefix = content[:brace]
        if prefix.strip() == "" or _TAG_TOKEN_RE.search(prefix):
            obj = _first_json_object(content)
            if obj is not None:
                c = _call_from_name_args(obj, source="embedded_json")
                if c is not None:
                    return [c]

    return []


def _loads(blob: str) -> Any:
    """``json.loads`` that returns ``None`` instead of raising on malformed input.

    RecursionError counts as malformed: deeply nested model output (or an
    echoed, prompt-injected tool result) must not escape the codec.
    """
    try:
        return json.loads(blob)
    except (ValueError, RecursionError):
        return None


def _strip_code_fence(s: str) -> str:
    m = _FENCE_RE.match(s.strip())
    return m.group(1).strip() if m else s


def _first_json_object(s: str) -> str | None:
    """Return the first brace-balanced ``{...}`` substring of ``s`` (ignoring braces
    inside JSON strings), tolerating trailing junk; ``None`` if none."""
    start = s.find("{")
    if start < 0:
        return None
    depth = 0
    in_str = esc = False
    for i in range(start, len(s)):
        ch = s[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        elif ch == '"':
            in_str = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return s[start:i + 1]
    return None


def _calls_from_blob(blob: str, *, source: ToolCallSource) -> list[ParsedToolCall]:
    """Parse a blob that is a single ``{"name":..}`` object OR a JSON array of them
    (used inside ``<tool_call>`` tags — Granite/Falcon wrap an array)."""
    blob = blob.strip().rstrip("`").strip()
    if blob.startswith("["):
        arr = _loads(blob)
        if not isinstance(arr, list):
            return []
        return [
            c for item in arr if isinstance(item, dict)
            for c in (_call_from_dict(item, source=source),) if c is not None
        ]
    c = _call_from_name_args(blob, source=source)
    return [c] if c is not None else []


def _call_from_name_args(blob: str, *, source: ToolCallSource) -> ParsedToolCall | None:
    """Parse ``{"name": ..., "arguments": {...}}`` JSON into a ParsedToolCall."""
    parsed = _loads(blob)
    if not isinstance(parsed, dict):
        return None
    return _call_from_dict(parsed, source=source)


def _call_from_dict(parsed: dict[str, Any], *, source: ToolCallSource) -> ParsedToolCall | None:
    """Build a ParsedToolCall from a decoded ``{"name":.., args..}`` dict.

    Accepts the arg-key aliases models use — OpenAI ``arguments``,
    Llama-3.x/Mistral ``parameters``, occasionally ``args`` — so a model that selects
    the right tool but names its args differently is not scored as an empty call.
    ``arguments`` may itself be a JSON string (double-encoded) — decode once more.
    """
    name = parsed.get("name", "")
    args = parsed.get("arguments")
    if args is None:
        args = parsed.get("parameters")
    if args is None:
        args = parsed.get("args", {})
    if isinstance(args, str):
        args = _loads(args)
    if not isinstance(name, str) or not name or not isinstance(args, dict):
        return None
    return ParsedToolCall(name=name, arguments=args, source=source)
