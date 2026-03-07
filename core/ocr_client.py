from __future__ import annotations

import asyncio
import json
import random
from dataclasses import dataclass
from typing import Any, Dict, Optional

import aiohttp

from core.image_utils import build_image_data_url


@dataclass
class OCRRequestConfig:
    api_base_url: str
    api_key: str
    model: str
    timeout_seconds: int = 120
    max_retries: int = 3
    retry_delay: float = 0.5


class OCRServiceError(Exception):
    """Structured OCR error with retry metadata and stable error code."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool,
        status_code: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.status_code = status_code
        self.details = details or {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "message": str(self),
            "retryable": self.retryable,
            "status_code": self.status_code,
            "details": self.details,
        }


def _extract_error_detail(error_text: str) -> str:
    if not error_text:
        return ""

    detail = error_text.strip()
    try:
        payload = json.loads(error_text)
    except json.JSONDecodeError:
        return detail

    if isinstance(payload, dict):
        error_obj = payload.get("error")
        if isinstance(error_obj, dict) and isinstance(error_obj.get("message"), str):
            return error_obj["message"].strip()
        if isinstance(payload.get("message"), str):
            return payload["message"].strip()

    return detail


def parse_markdown_content(raw: str) -> str:
    """Parse OCR model output and return display-ready text."""
    if not raw:
        return ""

    cleaned = raw.strip()
    if cleaned.startswith("```") and cleaned.endswith("```"):
        lines = cleaned.splitlines()
        if len(lines) >= 2:
            cleaned = "\n".join(lines[1:-1]).strip()
        else:
            cleaned = cleaned.strip("`").strip()

        lowered = cleaned.lower()
        if lowered.startswith("json"):
            cleaned = cleaned[4:].strip()
        elif lowered.startswith("markdown"):
            cleaned = cleaned[8:].strip()

    if not cleaned:
        return ""

    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        return cleaned

    if isinstance(data, dict):
        content = data.get("content") or data.get("text")
        if isinstance(content, str):
            return content.strip()
        return cleaned

    if isinstance(data, str):
        return data.strip()

    return cleaned


def _extract_raw_content(response_json: Dict[str, Any]) -> str:
    choices = response_json.get("choices")
    if not isinstance(choices, list) or not choices:
        raise OCRServiceError(
            "OCR_RESPONSE_SHAPE",
            "OCR响应缺少choices字段",
            retryable=False,
            details={"keys": list(response_json.keys()) if isinstance(response_json, dict) else []},
        )

    first_choice = choices[0]
    if not isinstance(first_choice, dict):
        raise OCRServiceError("OCR_RESPONSE_SHAPE", "OCR响应choices结构异常", retryable=False)

    message = first_choice.get("message")
    if not isinstance(message, dict):
        raise OCRServiceError("OCR_RESPONSE_SHAPE", "OCR响应缺少message字段", retryable=False)

    content = message.get("content")
    if isinstance(content, str):
        return content

    if isinstance(content, list):
        text_parts = []
        for block in content:
            if not isinstance(block, dict):
                continue
            block_type = block.get("type")
            if block_type in ("text", "output_text") and isinstance(block.get("text"), str):
                text_parts.append(block["text"])
        if text_parts:
            return "".join(text_parts)

    raise OCRServiceError("OCR_RESPONSE_SHAPE", "OCR响应内容为空或格式不支持", retryable=False)


def _build_payload(image_data: bytes, model: str, system_prompt: str, user_prompt: str) -> Dict[str, Any]:
    image_data_url = build_image_data_url(image_data)
    return {
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": user_prompt},
                    {"type": "image_url", "image_url": {"url": image_data_url}},
                ],
            },
        ],
        "stream": False,
        "model": model,
        "temperature": 0.0,
        "top_p": 1,
    }


def _validate_config(config: OCRRequestConfig, image_data: bytes) -> None:
    if not config.api_key:
        raise OCRServiceError("OCR_CONFIG_API_KEY", "API密钥未配置", retryable=False)
    if not config.api_base_url:
        raise OCRServiceError("OCR_CONFIG_BASE_URL", "API基础URL未配置", retryable=False)
    if not config.model:
        raise OCRServiceError("OCR_CONFIG_MODEL", "API模型未配置", retryable=False)
    if not image_data:
        raise OCRServiceError("OCR_EMPTY_IMAGE", "图片数据为空", retryable=False)


def _classify_http_error(status_code: int, detail: str) -> OCRServiceError:
    detail_suffix = f": {detail}" if detail else ""

    if status_code in (400,):
        return OCRServiceError(
            "OCR_BAD_REQUEST",
            f"OCR请求参数错误{detail_suffix}",
            retryable=False,
            status_code=status_code,
        )
    if status_code in (401, 403):
        return OCRServiceError(
            "OCR_AUTH_FAILED",
            f"OCR认证失败{detail_suffix}",
            retryable=False,
            status_code=status_code,
        )
    if status_code == 429:
        return OCRServiceError(
            "OCR_RATE_LIMIT",
            f"OCR请求触发限流{detail_suffix}",
            retryable=True,
            status_code=status_code,
        )
    if 500 <= status_code < 600:
        return OCRServiceError(
            "OCR_SERVER_ERROR",
            f"OCR服务端错误({status_code}){detail_suffix}",
            retryable=True,
            status_code=status_code,
        )

    return OCRServiceError(
        "OCR_HTTP_ERROR",
        f"OCR请求失败({status_code}){detail_suffix}",
        retryable=True,
        status_code=status_code,
    )


def _compute_retry_delay(base_delay: float, attempt_index: int) -> float:
    jitter = random.uniform(0.0, 0.3)
    max_delay = min(60.0, max(base_delay * 10, 1.0))
    return min(max_delay, (2 ** attempt_index) * max(base_delay, 0.1) + jitter)


async def request_ocr_markdown(
    config: OCRRequestConfig,
    image_data: bytes,
    *,
    system_prompt: str,
    user_prompt: str,
    shared_session: Optional[aiohttp.ClientSession] = None,
    logger=None,
    task_id: Optional[str] = None,
) -> str:
    """Send OCR request with unified retry/error policy and return markdown text."""
    _validate_config(config, image_data)

    if config.max_retries < 1:
        config.max_retries = 1

    timeout = aiohttp.ClientTimeout(total=config.timeout_seconds)
    created_session = shared_session is None
    session = shared_session

    if created_session:
        session = aiohttp.ClientSession(timeout=timeout)

    request_payload = _build_payload(image_data, config.model, system_prompt, user_prompt)
    request_url = f"{config.api_base_url}/v1/chat/completions"
    headers = {"Authorization": f"Bearer {config.api_key}"}

    last_error: Optional[OCRServiceError] = None
    context = f" task={task_id}" if task_id else ""

    try:
        for attempt in range(config.max_retries):
            try:
                response = await session.post(
                    request_url,
                    headers=headers,
                    json=request_payload,
                    timeout=timeout,
                )

                if response.status != 200:
                    error_text = await response.text()
                    detail = _extract_error_detail(error_text)
                    raise _classify_http_error(response.status, detail)

                response_json = await response.json()
                raw_content = _extract_raw_content(response_json)
                markdown = parse_markdown_content(raw_content)
                if not markdown:
                    raise OCRServiceError(
                        "OCR_EMPTY_CONTENT",
                        "OCR返回内容为空",
                        retryable=False,
                    )

                return markdown

            except OCRServiceError as exc:
                last_error = exc
            except asyncio.TimeoutError as exc:
                last_error = OCRServiceError(
                    "OCR_TIMEOUT",
                    "OCR请求超时",
                    retryable=True,
                    details={"exception": str(exc)},
                )
            except aiohttp.ClientError as exc:
                last_error = OCRServiceError(
                    "OCR_NETWORK_ERROR",
                    f"OCR网络错误: {exc}",
                    retryable=True,
                )
            except Exception as exc:
                last_error = OCRServiceError(
                    "OCR_UNEXPECTED_ERROR",
                    f"OCR未知错误: {exc}",
                    retryable=True,
                )

            if logger:
                logger.warning(
                    "ocr_request_failed%s attempt=%s/%s code=%s retryable=%s error=%s",
                    context,
                    attempt + 1,
                    config.max_retries,
                    last_error.code,
                    last_error.retryable,
                    str(last_error),
                )

            if (not last_error.retryable) or (attempt >= config.max_retries - 1):
                break

            await asyncio.sleep(_compute_retry_delay(config.retry_delay, attempt))

        assert last_error is not None
        raise last_error
    finally:
        if created_session and session:
            await session.close()
