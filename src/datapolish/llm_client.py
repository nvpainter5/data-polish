"""Provider-agnostic LLM client.

This is the only place in the codebase that knows which LLM provider we're
using. Everything else (the profiler, the cleaner, the agent) calls
`LLMClient.chat(...)` and stays portable.

Today: Groq.
Later (Task #12): Ollama for fully-local fallback.
Later (Phase 3a): Anthropic for the polished demo.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterable

from groq import Groq

from .config import settings


@dataclass
class ToolCall:
    """One tool invocation requested by the model."""

    id: str
    name: str
    arguments: dict


@dataclass
class ChatResponse:
    """Richer return shape for tool-calling chats.

    `text` is set when the model talks; `tool_calls` is set when it wants
    the orchestrator to execute one or more tools. Either or both can be
    populated in a single turn.
    """

    text: str | None = None
    tool_calls: list[ToolCall] = field(default_factory=list)


class LLMClient:
    """A thin wrapper around whichever provider is configured.

    Usage:
        client = LLMClient()
        reply = client.chat([
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "Hello!"},
        ])
    """

    def __init__(
        self,
        provider: str | None = None,
        model: str | None = None,
    ) -> None:
        self.provider = provider or settings.llm_provider
        self.model = model or settings.llm_model

        if self.provider == "groq":
            self._client = Groq(api_key=settings.groq_api_key)
        else:
            raise ValueError(
                f"Unknown LLM provider: {self.provider!r}. "
                "Supported: 'groq'. (Ollama and Anthropic coming later.)"
            )

    def chat(
        self,
        messages: Iterable[dict[str, str]],
        *,
        temperature: float = 0.0,
        max_tokens: int | None = None,
        **kwargs: Any,
    ) -> str:
        """Send a chat request and return the model's reply as a string.

        `temperature=0.0` is the default because for data cleaning we want
        deterministic, repeatable output — not creative variety.
        """
        if self.provider == "groq":
            response = self._client.chat.completions.create(
                model=self.model,
                messages=list(messages),
                temperature=temperature,
                max_tokens=max_tokens,
                **kwargs,
            )
            return response.choices[0].message.content or ""

        raise RuntimeError(f"Unreachable: provider={self.provider}")

    def chat_with_tools(
        self,
        messages: list[dict],
        tools: list[dict],
        *,
        temperature: float = 0.0,
        max_tokens: int | None = None,
        **kwargs: Any,
    ) -> ChatResponse:
        """Send a chat request with tool definitions; return text + tool_calls.

        Tools are described as OpenAI-compatible JSON-schema entries. The
        model decides whether to reply, call tools, or both.
        """
        if self.provider != "groq":
            raise NotImplementedError(
                f"chat_with_tools not yet implemented for {self.provider!r}"
            )

        response = self._client.chat.completions.create(
            model=self.model,
            messages=messages,
            tools=tools,
            tool_choice="auto",
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs,
        )
        msg = response.choices[0].message

        tool_calls: list[ToolCall] = []
        if msg.tool_calls:
            for tc in msg.tool_calls:
                try:
                    args = json.loads(tc.function.arguments)
                except json.JSONDecodeError:
                    args = {}
                tool_calls.append(
                    ToolCall(
                        id=tc.id,
                        name=tc.function.name,
                        arguments=args,
                    )
                )

        return ChatResponse(text=msg.content, tool_calls=tool_calls)
