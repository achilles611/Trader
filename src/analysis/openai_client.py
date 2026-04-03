from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from typing import Any

import requests

from .schema import build_response_format, validate_analysis_payload


RETRYABLE_STATUS_CODES = {408, 409, 425, 429, 500, 502, 503, 504}


class OpenAIAnalysisError(RuntimeError):
    pass


@dataclass(frozen=True)
class AnalysisResponse:
    response_id: str
    request_id: str
    model: str
    parsed_json: dict[str, Any]
    raw_response: dict[str, Any]
    prompt_cache_key: str
    request_tokens_est: int
    response_tokens_est: int
    request_size_bytes: int
    response_size_bytes: int
    latency_ms: float
    schema_validation_result: str


class OpenAIAnalysisClient:
    def __init__(self, settings, *, session: requests.Session | None = None) -> None:
        self.settings = settings
        self.session = session or requests.Session()

    def analyze_cycle(
        self,
        *,
        instructions: str,
        user_input: str,
        schema_document: dict[str, Any],
        metadata: dict[str, Any],
        previous_response_id: str | None = None,
    ) -> AnalysisResponse:
        if not self.settings.api_key:
            raise OpenAIAnalysisError("OPENAI_API_KEY is not configured.")

        payload = {
            "model": self.settings.model,
            "instructions": instructions,
            "input": user_input,
            "store": self.settings.store_responses,
            "metadata": metadata,
            "text": {
                "format": build_response_format(schema_document),
            },
        }
        if previous_response_id and self.settings.use_previous_response:
            payload["previous_response_id"] = previous_response_id

        optional_fields = {
            "prompt_cache_key": self.settings.prompt_cache_key,
        }
        if self.settings.prompt_cache_retention and self.settings.prompt_cache_retention != "in_memory":
            optional_fields["prompt_cache_retention"] = self.settings.prompt_cache_retention

        headers = {
            "Authorization": f"Bearer {self.settings.api_key}",
            "Content-Type": "application/json",
            "X-Client-Request-Id": str(uuid.uuid4()),
        }
        if self.settings.project_id:
            headers["OpenAI-Project"] = self.settings.project_id

        return self._post_with_retries(
            payload=payload,
            optional_fields=optional_fields,
            headers=headers,
            schema_document=schema_document,
        )

    def _post_with_retries(
        self,
        *,
        payload: dict[str, Any],
        optional_fields: dict[str, Any],
        headers: dict[str, str],
        schema_document: dict[str, Any],
    ) -> AnalysisResponse:
        attempts = self.settings.max_retries + 1
        current_payload = dict(payload)
        current_payload.update(optional_fields)
        allow_retry_without_optional_fields = bool(optional_fields)

        for attempt in range(1, attempts + 1):
            try:
                request_size_bytes = len(json.dumps(current_payload, sort_keys=True).encode("utf-8"))
                started = time.perf_counter()
                response = self.session.post(
                    "https://api.openai.com/v1/responses",
                    headers=headers,
                    json=current_payload,
                    timeout=self.settings.timeout_seconds,
                )
                latency_ms = (time.perf_counter() - started) * 1000
                if response.status_code in RETRYABLE_STATUS_CODES and attempt < attempts:
                    time.sleep(self.settings.backoff_seconds * attempt)
                    continue
                if response.status_code == 400 and allow_retry_without_optional_fields:
                    current_payload = dict(payload)
                    allow_retry_without_optional_fields = False
                    if attempt < attempts:
                        time.sleep(self.settings.backoff_seconds)
                        continue
                response.raise_for_status()
                raw_response = response.json()
                content = self._extract_output_text(raw_response)
                parsed = json.loads(content)
                validate_analysis_payload(parsed, schema_document)
                usage = raw_response.get("usage", {})
                return AnalysisResponse(
                    response_id=str(raw_response.get("id", "")),
                    request_id=response.headers.get("x-request-id", ""),
                    model=str(raw_response.get("model", self.settings.model)),
                    parsed_json=parsed,
                    raw_response=raw_response,
                    prompt_cache_key=current_payload.get("prompt_cache_key", ""),
                    request_tokens_est=int(usage.get("input_tokens", usage.get("prompt_tokens", 0)) or 0),
                    response_tokens_est=int(usage.get("output_tokens", usage.get("completion_tokens", 0)) or 0),
                    request_size_bytes=request_size_bytes,
                    response_size_bytes=len(response.content or b""),
                    latency_ms=latency_ms,
                    schema_validation_result="valid",
                )
            except (requests.RequestException, json.JSONDecodeError) as exc:
                if attempt < attempts:
                    time.sleep(self.settings.backoff_seconds * attempt)
                    continue
                raise OpenAIAnalysisError(str(exc)) from exc
        raise OpenAIAnalysisError("Responses API request exhausted retries without success.")

    def _extract_output_text(self, response_json: dict[str, Any]) -> str:
        output_text = response_json.get("output_text")
        if isinstance(output_text, str) and output_text.strip():
            return output_text
        for item in response_json.get("output", []):
            if item.get("type") != "message":
                continue
            for content in item.get("content", []):
                if content.get("type") in {"output_text", "text"}:
                    text = content.get("text") or content.get("value")
                    if isinstance(text, str) and text.strip():
                        return text
        raise OpenAIAnalysisError("Unable to extract text output from Responses API payload.")
