from __future__ import annotations

from typing import Any, Mapping, Optional

from core.ocr_client import OCRRequestConfig

OCR_SYSTEM_PROMPT = (
    "You are an expert OCR assistant. Extract all text from the provided image and preserve the original "
    "structure as clearly as possible. You may use Markdown when it helps readability, but return the OCR "
    "result directly as plain text without wrapping it in JSON."
)

OCR_USER_PROMPT = (
    "Analyze the provided image and return the recognized content directly. Do not add JSON wrappers, field "
    "names, or extra explanations."
)


def _as_positive_int(value: Any, default: int, minimum: int = 1) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return max(default, minimum)
    return max(parsed, minimum)


def _as_positive_float(value: Any, default: float, minimum: float = 0.1) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return max(default, minimum)
    return max(parsed, minimum)


def resolve_timeout_seconds(runtime_config: Any, default: int = 120) -> int:
    """Resolve OCR timeout from runtime config with backward-compatible fallbacks."""
    if runtime_config is None:
        return _as_positive_int(default, default)

    for field_name in ("api_request_timeout", "api_timeout"):
        if hasattr(runtime_config, field_name):
            value = getattr(runtime_config, field_name)
            try:
                parsed = int(value)
            except (TypeError, ValueError):
                continue
            if parsed > 0:
                return parsed

    return _as_positive_int(default, default)


def build_ocr_request_config(
    api_config: Optional[Mapping[str, Any]],
    runtime_config: Any,
    *,
    max_retries: int,
    default_timeout: int = 120,
    default_base_url: str = "https://api.openai.com",
    default_model: str = "gpt-4o",
    default_retry_delay: float = 0.5,
) -> OCRRequestConfig:
    """Create OCR request config with normalized timeout/retry values."""
    settings = api_config or {}

    api_base_url = str(settings.get("api_base_url") or default_base_url).strip()
    api_key = str(settings.get("api_key") or "").strip()
    model = str(settings.get("model") or default_model).strip()
    retry_delay = _as_positive_float(settings.get("retry_delay"), default_retry_delay)
    timeout_seconds = resolve_timeout_seconds(runtime_config, default_timeout)
    retries = _as_positive_int(max_retries, default=1, minimum=1)

    return OCRRequestConfig(
        api_base_url=api_base_url,
        api_key=api_key,
        model=model,
        timeout_seconds=timeout_seconds,
        max_retries=retries,
        retry_delay=retry_delay,
    )
