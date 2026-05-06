"""Provider-agnostic LLM client.

This is the only place in the codebase that knows which LLM provider we're
using. Everything else (the profiler, the cleaner, the agent) calls
`LLMClient.chat(...)` and stays portable.

Today: Groq.
Later (Task #12): Ollama for fully-local fallback.
Later (Phase 3a): Anthropic for the polished demo.
"""

from __future__ import annotations

from typing import Any, Iterable

from groq import Groq

from .config import settings


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
