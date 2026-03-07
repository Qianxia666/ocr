import base64
import logging
from io import BytesIO
from typing import Tuple

from PIL import Image, ImageOps

logger = logging.getLogger(__name__)

_FORMAT_TO_MIME = {
    "PNG": "image/png",
    "JPEG": "image/jpeg",
    "JPG": "image/jpeg",
    "WEBP": "image/webp",
    "GIF": "image/gif",
    "BMP": "image/bmp",
    "TIFF": "image/tiff",
}


def detect_image_mime_type(image_data: bytes, default: str = "image/png") -> str:
    """Best-effort MIME type detection for image bytes."""
    try:
        with Image.open(BytesIO(image_data)) as image:
            image_format = (image.format or "").upper()
        return _FORMAT_TO_MIME.get(image_format, default)
    except Exception:
        return default


def build_image_data_url(image_data: bytes) -> str:
    """Build a base64 data URL using the actual image MIME type."""
    encoded_image = base64.b64encode(image_data).decode("utf-8")
    mime_type = detect_image_mime_type(image_data)
    return f"data:{mime_type};base64,{encoded_image}"


def _prepare_image_for_compression(image: Image.Image) -> Image.Image:
    working_image = ImageOps.exif_transpose(image)

    if working_image.mode in ("RGBA", "LA") or (
        working_image.mode == "P" and "transparency" in working_image.info
    ):
        alpha_image = working_image.convert("RGBA")
        background = Image.new("RGBA", alpha_image.size, (255, 255, 255, 255))
        return Image.alpha_composite(background, alpha_image).convert("RGB")

    if working_image.mode not in ("RGB", "L"):
        return working_image.convert("RGB")

    return working_image.copy()


def _save_png_bytes(image: Image.Image) -> bytes:
    with BytesIO() as output_buffer:
        image.save(output_buffer, format="PNG", optimize=True)
        return output_buffer.getvalue()


def _save_jpeg_bytes(image: Image.Image, quality: int) -> bytes:
    jpeg_image = image if image.mode == "RGB" else image.convert("RGB")
    with BytesIO() as output_buffer:
        jpeg_image.save(
            output_buffer,
            format="JPEG",
            quality=quality,
            optimize=True,
            progressive=True,
            subsampling=2,
        )
        return output_buffer.getvalue()


def _find_best_jpeg_fit(image: Image.Image, target_size: int) -> Tuple[bytes, int]:
    min_quality = 20
    max_quality = 95

    smallest_candidate = _save_jpeg_bytes(image, min_quality)
    if len(smallest_candidate) > target_size:
        return smallest_candidate, min_quality

    best_bytes = smallest_candidate
    best_quality = min_quality
    low = min_quality
    high = max_quality

    while low <= high:
        quality = (low + high) // 2
        candidate = _save_jpeg_bytes(image, quality)
        if len(candidate) <= target_size:
            best_bytes = candidate
            best_quality = quality
            low = quality + 1
        else:
            high = quality - 1

    return best_bytes, best_quality


def compress_image_to_target_ratio(image_data: bytes, target_ratio: float) -> bytes:
    """Compress image bytes toward a target output-size ratio.

    The ratio expresses desired output bytes divided by original bytes. The
    implementation first tries lossless PNG optimization, then uses JPEG quality
    search, and only scales down as a fallback when quality alone cannot reach
    the target size.
    """
    try:
        if not image_data:
            return image_data

        target_ratio = max(0.1, min(1.0, float(target_ratio)))
        if target_ratio >= 1.0:
            return image_data

        original_size = len(image_data)
        target_size = max(1, int(original_size * target_ratio))
        if target_size >= original_size:
            return image_data

        with Image.open(BytesIO(image_data)) as source_image:
            working_image = _prepare_image_for_compression(source_image)
            original_dimensions = working_image.size

            best_bytes = None
            best_format = "original"
            best_distance = None

            for _ in range(6):
                png_bytes = _save_png_bytes(working_image)
                if len(png_bytes) <= target_size:
                    best_bytes = png_bytes
                    best_format = "PNG"
                    break

                jpeg_bytes, _jpeg_quality = _find_best_jpeg_fit(working_image, target_size)
                current_distance = abs(len(jpeg_bytes) - target_size)

                if best_bytes is None or current_distance < best_distance or len(jpeg_bytes) < len(best_bytes):
                    best_bytes = jpeg_bytes
                    best_format = "JPEG"
                    best_distance = current_distance

                if len(jpeg_bytes) <= target_size:
                    break

                if working_image.width <= 64 or working_image.height <= 64:
                    break

                resize_ratio = (target_size / len(jpeg_bytes)) ** 0.5 * 0.98
                resize_ratio = max(0.5, min(0.95, resize_ratio))
                new_width = max(1, int(working_image.width * resize_ratio))
                new_height = max(1, int(working_image.height * resize_ratio))

                if new_width == working_image.width and new_height == working_image.height:
                    break

                working_image = working_image.resize((new_width, new_height), Image.LANCZOS)

        if best_bytes and len(best_bytes) < original_size:
            size_saved_percent = (1 - len(best_bytes) / original_size) * 100
            final_width = max(1, working_image.size[0])
            final_height = max(1, working_image.size[1])
            logger.info(
                "Image compression complete: %sx%s -> %sx%s, original: %sKB, compressed: %sKB, saved: %.1f%%, target ratio: %.2f, output format: %s",
                original_dimensions[0],
                original_dimensions[1],
                final_width,
                final_height,
                original_size // 1024,
                len(best_bytes) // 1024,
                size_saved_percent,
                target_ratio,
                best_format,
            )
            return best_bytes

    except Exception as exc:
        logger.warning("Image compression failed, using original bytes: %s", exc)

    return image_data
