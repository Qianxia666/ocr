import logging
import os
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class RuntimeConfig:
    """Runtime-tunable settings stored in memory."""

    def __init__(self):
        self.concurrency = int(os.getenv("CONCURRENCY", 5))
        self.max_retries = int(os.getenv("MAX_RETRIES", 5))
        self.image_concurrency = int(os.getenv("IMAGE_CONCURRENCY", 5))
        self.pdf_concurrency = int(os.getenv("PDF_CONCURRENCY", 5))
        self.pdf_dpi = int(os.getenv("PDF_DPI", 200))
        self.batch_size = int(os.getenv("BATCH_SIZE", 8))
        self.max_image_size_mb = int(os.getenv("MAX_IMAGE_SIZE_MB", 50))
        self.max_pdf_size_mb = int(os.getenv("MAX_PDF_SIZE_MB", 50))
        self.max_pdf_pages = int(os.getenv("MAX_PDF_PAGES", 50))

        self.api_timeout = int(os.getenv("API_TIMEOUT", 300))
        self.api_request_timeout = int(os.getenv("API_REQUEST_TIMEOUT", 300))
        self.frontend_poll_timeout = int(os.getenv("FRONTEND_POLL_TIMEOUT", 300))
        self.image_compression_ratio = float(os.getenv("IMAGE_COMPRESSION_RATIO", 0.2))

    def get_all(self) -> Dict[str, Any]:
        return {
            "concurrency": self.concurrency,
            "max_retries": self.max_retries,
            "image_concurrency": self.image_concurrency,
            "pdf_concurrency": self.pdf_concurrency,
            "pdf_dpi": self.pdf_dpi,
            "batch_size": self.batch_size,
            "max_image_size_mb": self.max_image_size_mb,
            "max_pdf_size_mb": self.max_pdf_size_mb,
            "max_pdf_pages": self.max_pdf_pages,
            "api_timeout": self.api_timeout,
            "api_request_timeout": self.api_request_timeout,
            "frontend_poll_timeout": self.frontend_poll_timeout,
            "image_compression_ratio": self.image_compression_ratio,
        }

    def update(self, config: Dict[str, Any]):
        if "concurrency" in config:
            self.concurrency = int(config["concurrency"])
        if "max_retries" in config:
            self.max_retries = int(config["max_retries"])
        if "image_concurrency" in config:
            self.image_concurrency = int(config["image_concurrency"])
        if "pdf_concurrency" in config:
            self.pdf_concurrency = int(config["pdf_concurrency"])
        if "pdf_dpi" in config:
            self.pdf_dpi = int(config["pdf_dpi"])
        if "batch_size" in config:
            self.batch_size = int(config["batch_size"])
        if "max_image_size_mb" in config:
            self.max_image_size_mb = int(config["max_image_size_mb"])
        if "max_pdf_size_mb" in config:
            self.max_pdf_size_mb = int(config["max_pdf_size_mb"])
        if "max_pdf_pages" in config:
            self.max_pdf_pages = int(config["max_pdf_pages"])
        if "api_timeout" in config:
            self.api_timeout = int(config["api_timeout"])
            logger.info("Updated API timeout: %s seconds", self.api_timeout)
        if "api_request_timeout" in config:
            self.api_request_timeout = int(config["api_request_timeout"])
            logger.info("Updated API request timeout: %s seconds", self.api_request_timeout)
        if "frontend_poll_timeout" in config:
            self.frontend_poll_timeout = int(config["frontend_poll_timeout"])
            logger.info("Updated frontend poll timeout: %s seconds", self.frontend_poll_timeout)
        if "image_compression_ratio" in config:
            ratio = float(config["image_compression_ratio"])
            self.image_compression_ratio = max(0.1, min(1.0, ratio))
            logger.info("Updated image target size ratio: %s", self.image_compression_ratio)


def _mask_secret(value: str, visible: int = 4) -> str:
    """Mask sensitive values for UI/API output."""
    if not value:
        return ""
    value = value.strip()
    if len(value) <= visible * 2:
        return "*" * len(value)
    return f"{value[:visible]}****{value[-visible:]}"


class ApiRuntimeConfig:
    """API settings with ENV defaults and runtime overrides."""

    def __init__(self, api_base_url: str, api_key: str, model: str):
        self.env_config = {
            "api_base_url": (api_base_url or "").strip(),
            "api_key": (api_key or "").strip(),
            "model": (model or "").strip(),
        }
        self.runtime_config: Dict[str, str] = {}
        logger.info(
            "API config initialized from ENV: base_url=%s, model=%s",
            self.env_config.get("api_base_url"),
            self.env_config.get("model"),
        )

    def _normalize(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            return value.strip()
        return str(value)

    def get_config(self, key: str) -> str:
        value = self.runtime_config.get(key)
        if value is not None:
            return value
        return self.env_config.get(key, "")

    def has_runtime_override(self, key: str) -> bool:
        return key in self.runtime_config

    def update_runtime(self, config: Dict[str, Any]):
        updates: Dict[str, str] = {}
        for key in ("api_base_url", "api_key", "model"):
            if key in config:
                normalized = self._normalize(config.get(key))
                if normalized is None:
                    continue
                if key == "api_key" and normalized == "":
                    continue
                updates[key] = normalized

        if config.get("clear_api_key"):
            updates["api_key"] = ""

        if updates:
            self.runtime_config.update(updates)
            safe_updates = {k: ("***" if k == "api_key" else v) for k, v in updates.items()}
            logger.info("Runtime API config updated: %s", safe_updates)

    def reset_runtime(self):
        self.runtime_config = {}
        logger.info("Runtime API config reset to ENV defaults")

    def get_effective_config(self, include_sensitive: bool = True) -> Dict[str, str]:
        config = self.env_config.copy()
        config.update(self.runtime_config)
        if not include_sensitive:
            config.pop("api_key", None)
        return config

    def get_sources(self) -> Dict[str, str]:
        return {
            key: ("runtime" if key in self.runtime_config else "env")
            for key in ("api_base_url", "api_key", "model")
        }


def _parse_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    value_str = str(value).strip().lower()
    if value_str in ("true", "1", "yes", "on"):
        return True
    if value_str in ("false", "0", "no", "off"):
        return False
    return None


class TurnstileRuntimeConfig:
    """Turnstile settings with ENV defaults and runtime overrides."""

    def __init__(self, site_key: str, secret_key: str, env_enabled_raw: Optional[str] = None):
        if env_enabled_raw is None:
            env_enabled_raw = os.getenv("TURNSTILE_ENABLED")

        parsed_env_enabled = _parse_bool(env_enabled_raw)
        self.env_enabled_source = "env" if env_enabled_raw is not None else "default"
        self.env_config = {
            "enabled": parsed_env_enabled if parsed_env_enabled is not None else False,
            "site_key": (site_key or "").strip(),
            "secret_key": (secret_key or "").strip(),
        }
        self.runtime_config: Dict[str, Any] = {}

        logger.info(
            "Turnstile config initialized: enabled=%s, site_key_set=%s, secret_set=%s",
            self.env_config["enabled"],
            bool(self.env_config["site_key"]),
            bool(self.env_config["secret_key"]),
        )

    def update_runtime(self, config: Dict[str, Any]):
        updates: Dict[str, Any] = {}

        if "enabled" in config:
            parsed = _parse_bool(config.get("enabled"))
            if parsed is not None:
                updates["enabled"] = parsed

        if "site_key" in config and config["site_key"] is not None:
            updates["site_key"] = str(config["site_key"]).strip()

        if "secret_key" in config and config["secret_key"] is not None:
            secret_value = str(config["secret_key"]).strip()
            if secret_value:
                updates["secret_key"] = secret_value

        if config.get("clear_secret_key"):
            updates["secret_key"] = ""

        if updates:
            safe_updates = updates.copy()
            if "secret_key" in safe_updates:
                safe_updates["secret_key"] = "***"
            self.runtime_config.update(updates)
            logger.info("Runtime Turnstile config updated: %s", safe_updates)

    def reset_runtime(self):
        self.runtime_config = {}
        logger.info("Runtime Turnstile config reset to ENV defaults")

    def get_config(self, key: str):
        if key in self.runtime_config:
            return self.runtime_config[key]
        return self.env_config.get(key)

    def has_runtime_override(self, key: str) -> bool:
        return key in self.runtime_config

    def get_enabled_with_source(self) -> tuple[bool, str]:
        if "enabled" in self.runtime_config:
            return bool(self.runtime_config["enabled"]), "runtime"

        if self.env_enabled_source == "env":
            return bool(self.env_config.get("enabled", False)), "env"

        return bool(self.env_config.get("enabled", False)), "default"

    def get_effective_config(self, include_sensitive: bool = True) -> Dict[str, Any]:
        config = self.env_config.copy()
        config.update(self.runtime_config)
        if not include_sensitive:
            config.pop("secret_key", None)
        return config

    def get_sources(self) -> Dict[str, str]:
        sources: Dict[str, str] = {}
        for key in ("enabled", "site_key", "secret_key"):
            if key in self.runtime_config:
                sources[key] = "runtime"
            else:
                sources[key] = "env" if self.env_config.get(key) else "default"
        return sources

    def has_secret_key(self) -> bool:
        if "secret_key" in self.runtime_config:
            return bool(self.runtime_config["secret_key"])
        return bool(self.env_config.get("secret_key"))
