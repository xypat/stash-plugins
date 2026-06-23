import json
import math
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

MAX_QUALITY = 95
PREFERRED_MIN_QUALITY = 75
MIN_DIMENSION = 160


@dataclass(frozen=True)
class ImageInfo:
    width: int
    height: int


@dataclass(frozen=True)
class CompressedCover:
    data: bytes
    quality: int
    width: int
    height: int


def resolve_executable(configured_path: str | None, name: str) -> str:
    if configured_path:
        path = Path(configured_path)
        if path.is_file():
            return str(path)

    discovered = shutil.which(name)
    if discovered:
        return discovered

    detail = f"; Stash returned path: {configured_path}" if configured_path else ""
    raise RuntimeError(f"Could not find {name}{detail}")


def _run(command: list[str], timeout: int = 120) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(
            command,
            capture_output=True,
            check=False,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"Command timed out: {command[0]}") from exc
    except OSError as exc:
        raise RuntimeError(f"Could not execute {command[0]}: {exc}") from exc


def validate_ffmpeg(ffmpeg_path: str) -> None:
    encoders = _run([ffmpeg_path, "-hide_banner", "-encoders"])
    muxers = _run([ffmpeg_path, "-hide_banner", "-muxers"])
    encoder_text = (encoders.stdout + encoders.stderr).decode("utf-8", errors="replace")
    muxer_text = (muxers.stdout + muxers.stderr).decode("utf-8", errors="replace")

    if encoders.returncode != 0 or "libaom-av1" not in encoder_text:
        raise RuntimeError("FFmpeg does not include the libaom-av1 encoder required for AVIF")
    if muxers.returncode != 0 or " avif " not in f" {muxer_text} ":
        raise RuntimeError("FFmpeg does not include the AVIF muxer")


def probe_image(ffprobe_path: str, input_path: Path) -> ImageInfo:
    result = _run(
        [
            ffprobe_path,
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=width,height",
            "-of",
            "json",
            str(input_path),
        ]
    )
    if result.returncode != 0:
        message = result.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"FFprobe could not read the cover: {message or 'unknown error'}")

    payload = json.loads(result.stdout)
    streams = payload.get("streams") or []
    if not streams:
        raise RuntimeError("The cover does not contain a readable image stream")

    width = int(streams[0].get("width") or 0)
    height = int(streams[0].get("height") or 0)
    if width <= 0 or height <= 0:
        raise RuntimeError("Could not determine the cover dimensions")
    return ImageInfo(width=width, height=height)


def quality_to_crf(quality: int) -> int:
    return max(0, min(63, round((100 - quality) * 63 / 100)))


def scaled_dimensions(info: ImageInfo, max_width: int, scale: float) -> ImageInfo:
    initial_scale = min(1.0, max_width / info.width)
    width = max(2, round(info.width * initial_scale * scale))
    height = max(2, round(info.height * initial_scale * scale))
    width -= width % 2
    height -= height % 2
    return ImageInfo(width=width, height=height)


def encode_avif(
    ffmpeg_path: str,
    input_path: Path,
    output_path: Path,
    dimensions: ImageInfo,
    quality: int,
) -> bytes:
    output_path.unlink(missing_ok=True)
    result = _run(
        [
            ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            str(input_path),
            "-map_metadata",
            "-1",
            "-frames:v",
            "1",
            "-vf",
            f"scale={dimensions.width}:{dimensions.height}:flags=lanczos",
            "-c:v",
            "libaom-av1",
            "-still-picture",
            "1",
            "-crf",
            str(quality_to_crf(quality)),
            "-b:v",
            "0",
            "-cpu-used",
            "4",
            "-row-mt",
            "1",
            "-pix_fmt",
            "yuv420p",
            "-f",
            "avif",
            str(output_path),
        ],
        timeout=180,
    )
    if result.returncode != 0 or not output_path.exists():
        message = result.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"FFmpeg failed to encode AVIF: {message or 'unknown error'}")
    return output_path.read_bytes()


def compress_cover(
    source: bytes,
    ffmpeg_path: str,
    ffprobe_path: str,
    target_bytes: int,
    min_quality: int,
    max_width: int,
) -> CompressedCover:
    with tempfile.TemporaryDirectory(prefix="stash-cover-") as temp_dir:
        input_path = Path(temp_dir) / "source-image"
        output_path = Path(temp_dir) / "cover.avif"
        input_path.write_bytes(source)
        source_info = probe_image(ffprobe_path, input_path)

        fallback: CompressedCover | None = None
        scale = 1.0

        while True:
            dimensions = scaled_dimensions(source_info, max_width, scale)
            if (
                scale < 1.0
                and dimensions.width < MIN_DIMENSION
                and dimensions.height < MIN_DIMENSION
            ):
                break

            low = min_quality
            high = MAX_QUALITY
            best: tuple[bytes, int] | None = None
            while low <= high:
                quality = math.floor((low + high) / 2)
                encoded = encode_avif(
                    ffmpeg_path,
                    input_path,
                    output_path,
                    dimensions,
                    quality,
                )
                if len(encoded) <= target_bytes:
                    best = (encoded, quality)
                    low = quality + 1
                else:
                    high = quality - 1

            if best:
                candidate = CompressedCover(
                    data=best[0],
                    quality=best[1],
                    width=dimensions.width,
                    height=dimensions.height,
                )
                if candidate.quality >= PREFERRED_MIN_QUALITY:
                    return candidate
                if (
                    fallback is None
                    or candidate.quality > fallback.quality
                    or (
                        candidate.quality == fallback.quality
                        and candidate.width * candidate.height > fallback.width * fallback.height
                    )
                ):
                    fallback = candidate

            scale *= 0.9

        if fallback:
            return fallback

    raise RuntimeError(
        f"Could not compress the cover to {target_bytes / 1024:.1f} KiB "
        f"with minimum quality {min_quality} and minimum dimension {MIN_DIMENSION}px"
    )
