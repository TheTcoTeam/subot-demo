import os
import sys
import re
import json
import time
import math
import glob
import hashlib
import asyncio
import subprocess
import concurrent.futures

import httpx
from deep_translator import GoogleTranslator
from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    AuthKeyError,
    AuthKeyDuplicatedError,
    AuthKeyUnregisteredError,
    FileReferenceExpiredError,
)
from telethon.network import MTProtoSender
from telethon.tl.types import (
    DocumentAttributeVideo,
    InputDocumentFileLocation,
    InputFile,
    InputFileBig,
)
from telethon.tl.functions.upload import (
    GetFileRequest,
    SaveFilePartRequest,
    SaveBigFilePartRequest,
)

from groq import Groq

# ================= INPUTS & SECRETS =================
API_ID = int((os.getenv("TG_API_ID") or "2040").strip())
API_HASH = (os.getenv("TG_API_HASH") or "b18441a1ff607e10a989891a5462e627").strip()
BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
GROQ_API_KEY = (os.getenv("GROQ_API_KEY") or "").strip()
GEMINI_API_KEY = (os.getenv("GEMINI_API_KEY") or "").strip()

CHAT_ID = int((os.getenv("CHAT_ID") or "0").strip())
STATUS_MSG_ID = int((os.getenv("STATUS_MSG_ID") or "0").strip())
VIDEO_MSG_ID = int((os.getenv("VIDEO_MSG_ID") or "0").strip())
THUMB_MSG_ID = int((os.getenv("THUMB_MSG_ID") or "0").strip())
USER_TAG = (os.getenv("USER_TAG") or "@YourChannelTag").strip()

groq_client = Groq(api_key=GROQ_API_KEY, max_retries=1, timeout=30.0) if GROQ_API_KEY else None

last_edit_time = 0.0
_status_lock = asyncio.Lock()


# ================= PORTABLE I/O HELPERS =================
def safe_pread(fd: int, size: int, offset: int) -> bytes:
    """Reads bytes at an offset with fallback for systems lacking os.pread."""
    if hasattr(os, "pread"):
        return os.pread(fd, size, offset)
    pos = os.lseek(fd, 0, os.SEEK_CUR)
    os.lseek(fd, offset, os.SEEK_SET)
    data = os.read(fd, size)
    os.lseek(fd, pos, os.SEEK_SET)
    return data


def safe_pwrite(fd: int, data: bytes, offset: int):
    """Writes bytes at an offset with fallback for systems lacking os.pwrite."""
    if hasattr(os, "pwrite"):
        os.pwrite(fd, data, offset)
    else:
        pos = os.lseek(fd, 0, os.SEEK_CUR)
        os.lseek(fd, offset, os.SEEK_SET)
        os.write(fd, data)
        os.lseek(fd, pos, os.SEEK_SET)


# ================= NON-BLOCKING STATUS & PROGRESS HELPERS =================
async def _do_edit_status(client: TelegramClient, step_text: str):
    try:
        text = (
            "⚡ <b>موتور پردازش فوق‌سریع زیرنویس (نسخه توربو پرو)</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"{step_text}\n"
            "━━━━━━━━━━━━━━━━━━━━"
        )
        await client.edit_message(CHAT_ID, STATUS_MSG_ID, text, parse_mode="html")
    except Exception:
        pass


async def update_status(client: TelegramClient, step_text: str, force: bool = True):
    global last_edit_time
    now = time.time()
    if not force and (now - last_edit_time) < 3.5:
        return
    last_edit_time = now
    if force:
        await _do_edit_status(client, step_text)
    else:
        # Fire-and-forget so download/upload/encode loops never pause waiting for Telegram HTTP/MTProto roundtrip
        asyncio.create_task(_do_edit_status(client, step_text))


def make_progress_bar(percent: float) -> str:
    filled = int(round(percent / 10))
    filled = max(0, min(10, filled))
    return "🟩" * filled + "⬜" * (10 - filled)


# ================= FLOOD-FREE 6-SOCKET MTPROTO SENDER ENGINE =================
async def create_sender_pool(client: TelegramClient, dc_id: int, pool_size: int = 6) -> list[MTProtoSender]:
    dc = await client._get_dc(dc_id)
    senders = []

    if dc_id == client.session.dc_id:
        for _ in range(pool_size):
            sender = MTProtoSender(client.session.auth_key, loggers=client._log)
            await sender.connect(
                client._connection(
                    dc.ip_address,
                    dc.port,
                    dc.id,
                    loggers=client._log,
                    proxy=client._proxy
                )
            )
            senders.append(sender)
    else:
        first_sender = await client._borrow_exported_sender(dc_id)
        shared_auth_key = first_sender.auth_key
        await client._return_exported_sender(first_sender)

        for _ in range(pool_size):
            sender = MTProtoSender(shared_auth_key, loggers=client._log)
            await sender.connect(
                client._connection(
                    dc.ip_address,
                    dc.port,
                    dc.id,
                    loggers=client._log,
                    proxy=client._proxy
                )
            )
            senders.append(sender)

    return senders


async def disconnect_sender_pool(senders: list[MTProtoSender]):
    await asyncio.gather(*(s.disconnect() for s in senders), return_exceptions=True)


async def striped_download_file(
    client: TelegramClient,
    media_message,
    output_path: str,
    progress_callback=None,
    pool_size: int = 6
):
    doc = media_message.document or media_message.video
    if not doc:
        return await client.download_media(media_message, file=output_path, progress_callback=progress_callback)

    file_size = doc.size
    part_size = 512 * 1024  # 512 KB
    total_parts = math.ceil(file_size / part_size)
    dc_id = doc.dc_id

    location = InputDocumentFileLocation(
        id=doc.id,
        access_hash=doc.access_hash,
        file_reference=doc.file_reference,
        thumb_size=""
    )

    try:
        senders = await create_sender_pool(client, dc_id, pool_size=pool_size)
    except Exception as pool_err:
        print(f"[WORKER] Sender pool fallback to standard download: {pool_err}", flush=True)
        return await client.download_media(media_message, file=output_path, progress_callback=progress_callback)

    fd = os.open(output_path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
    os.ftruncate(fd, file_size)

    downloaded_bytes = 0
    queue = asyncio.Queue()
    for part_idx in range(total_parts):
        queue.put_nowait(part_idx)

    async def worker_task(sender: MTProtoSender):
        nonlocal downloaded_bytes, location
        while not queue.empty():
            try:
                part_idx = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            offset = part_idx * part_size
            for attempt in range(5):
                try:
                    res = await client._call(
                        sender,
                        GetFileRequest(location=location, offset=offset, limit=part_size)
                    )
                    chunk = res.bytes
                    safe_pwrite(fd, chunk, offset)
                    downloaded_bytes += len(chunk)
                    if progress_callback:
                        await progress_callback(downloaded_bytes, file_size)
                    break
                except FloodWaitError as fw:
                    await asyncio.sleep(min(fw.seconds, 15) + 0.5)
                except FileReferenceExpiredError:
                    refreshed = await client.get_messages(CHAT_ID, ids=VIDEO_MSG_ID)
                    new_doc = refreshed.document or refreshed.video
                    location = InputDocumentFileLocation(
                        id=new_doc.id,
                        access_hash=new_doc.access_hash,
                        file_reference=new_doc.file_reference,
                        thumb_size=""
                    )
                except Exception as e:
                    if attempt == 4:
                        raise e
                    await asyncio.sleep(0.35 * (attempt + 1))
            queue.task_done()

    try:
        tasks = [asyncio.create_task(worker_task(s)) for s in senders]
        await asyncio.gather(*tasks)
    except Exception as dl_err:
        print(f"[WORKER] Striped download error ({dl_err}), falling back to native download...", flush=True)
        os.close(fd)
        await disconnect_sender_pool(senders)
        return await client.download_media(media_message, file=output_path, progress_callback=progress_callback)
    else:
        os.close(fd)
        await disconnect_sender_pool(senders)

    return output_path


async def striped_upload_file(
    client: TelegramClient,
    file_path: str,
    progress_callback=None,
    pool_size: int = 6
):
    file_size = os.path.getsize(file_path)
    part_size = 512 * 1024  # 512 KB
    total_parts = math.ceil(file_size / part_size)
    is_big = file_size > 10 * 1024 * 1024
    file_id = int.from_bytes(os.urandom(8), signed=True)

    hash_md5 = hashlib.md5() if not is_big else None
    if not is_big:
        with open(file_path, "rb") as f:
            while chunk := f.read(65536):
                hash_md5.update(chunk)

    try:
        senders = await create_sender_pool(client, client.session.dc_id, pool_size=pool_size)
    except Exception as pool_err:
        print(f"[WORKER] Upload sender pool fallback: {pool_err}", flush=True)
        return await client.upload_file(file_path, progress_callback=progress_callback)

    fd = os.open(file_path, os.O_RDONLY)

    uploaded_bytes = 0
    queue = asyncio.Queue()
    for part_idx in range(total_parts):
        queue.put_nowait(part_idx)

    async def worker_task(sender: MTProtoSender):
        nonlocal uploaded_bytes
        while not queue.empty():
            try:
                part_idx = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            offset = part_idx * part_size
            chunk = safe_pread(fd, part_size, offset)

            for attempt in range(5):
                try:
                    if is_big:
                        req = SaveBigFilePartRequest(
                            file_id=file_id,
                            file_part=part_idx,
                            file_total_parts=total_parts,
                            bytes=chunk
                        )
                    else:
                        req = SaveFilePartRequest(
                            file_id=file_id,
                            file_part=part_idx,
                            bytes=chunk
                        )
                    await client._call(sender, req)
                    uploaded_bytes += len(chunk)
                    if progress_callback:
                        await progress_callback(uploaded_bytes, file_size)
                    break
                except FloodWaitError as fw:
                    await asyncio.sleep(min(fw.seconds, 15) + 0.5)
                except Exception as e:
                    if attempt == 4:
                        raise e
                    await asyncio.sleep(0.35 * (attempt + 1))
            queue.task_done()

    try:
        tasks = [asyncio.create_task(worker_task(s)) for s in senders]
        await asyncio.gather(*tasks)
    except Exception as ul_err:
        print(f"[WORKER] Striped upload error ({ul_err}), falling back to native upload...", flush=True)
        os.close(fd)
        await disconnect_sender_pool(senders)
        return await client.upload_file(file_path, progress_callback=progress_callback)
    else:
        os.close(fd)
        await disconnect_sender_pool(senders)

    file_name = os.path.basename(file_path)
    if is_big:
        return InputFileBig(id=file_id, parts=total_parts, name=file_name)
    else:
        return InputFile(
            id=file_id,
            parts=total_parts,
            name=file_name,
            md5_checksum=hash_md5.hexdigest()
        )


# ================= VIDEO & AUDIO PROCESSING =================
def get_video_info(video_path: str) -> tuple[int, int, int, float, int]:
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=width,height,r_frame_rate,bit_rate,tags,side_data_list:format=duration,bit_rate",
        "-of", "json", video_path
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    data = json.loads(res.stdout)
    stream = data["streams"][0]
    fmt = data.get("format", {})

    width = int(stream.get("width", 1920))
    height = int(stream.get("height", 1080))

    # Detect mobile rotation metadata (90 / 270 deg) so vertical phone videos aren't stretched!
    rotation = 0
    tags = stream.get("tags", {}) or {}
    if "rotate" in tags:
        try:
            rotation = abs(int(float(tags["rotate"]))) % 360
        except Exception:
            pass
    for sd in stream.get("side_data_list", []) or []:
        if "rotation" in sd:
            try:
                rotation = abs(int(float(sd["rotation"]))) % 360
            except Exception:
                pass
    if rotation in (90, 270):
        width, height = height, width

    duration = max(1, int(float(fmt.get("duration", 1))))

    fps_str = stream.get("r_frame_rate", "30/1")
    try:
        num, den = fps_str.split("/")
        fps = float(num) / float(den)
    except Exception:
        fps = 30.0

    raw_br = stream.get("bit_rate") or fmt.get("bit_rate")
    if raw_br:
        bitrate_kbps = int(int(raw_br) / 1000)
        if not stream.get("bit_rate"):
            bitrate_kbps = max(300, bitrate_kbps - 128)
    else:
        file_bytes = os.path.getsize(video_path)
        total_kbps = int((file_bytes * 8) / duration / 1000)
        bitrate_kbps = max(300, total_kbps - 128)

    return width, height, duration, fps, bitrate_kbps


def compute_target_resolution(width: int, height: int) -> tuple[int, int]:
    """
    Preserves original 1080p Full HD resolution and below (landscape or portrait/vertical).
    Only downscales ultra-high 4K/2K sources (>1080p) to 1080p for reliable Telegram mobile playback.
    """
    min_dim = min(width, height)
    if min_dim <= 1080:
        return (width - (width % 2), height - (height % 2))
    scale_factor = 1080.0 / float(min_dim)
    new_w = int(round(width * scale_factor))
    new_h = int(round(height * scale_factor))
    new_w -= (new_w % 2)
    new_h -= (new_h % 2)
    return new_w, new_h


def extract_ultrafast_audio(video_path: str, audio_path: str):
    """
    Extracts high-efficiency 16kHz mono MP3 audio (32kbps) using fastest LAME algorithm (-q:a 9).
    """
    cmd = [
        "ffmpeg", "-y", "-nostdin", "-threads", "0", "-i", video_path,
        "-vn", "-sn", "-ac", "1", "-ar", "16000",
        "-c:a", "libmp3lame", "-b:a", "32k", "-q:a", "9",
        audio_path
    ]
    res = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    if res.returncode != 0:
        cmd_fallback = [
            "ffmpeg", "-y", "-nostdin", "-threads", "0", "-i", video_path,
            "-vn", "-sn", "-ac", "1", "-ar", "16000", "-c:a", "aac", "-b:a", "32k",
            audio_path
        ]
        subprocess.run(cmd_fallback, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def post_process_subtitle_timings(segments: list[dict]) -> list[dict]:
    """
    1. Automatically cuts off subtitles during silence so short phrases don't stay stuck on screen.
    2. Eliminates overlapping timestamps between consecutive lines.
    """
    if not segments:
        return []

    cleaned = []
    for idx, seg in enumerate(segments):
        start = float(seg["start"])
        end = float(seg["end"])
        text = seg["text"].strip()
        if not text or end <= start:
            continue

        # Cap abnormally stretched durations when Whisper holds a short phrase across silence
        word_count = max(1, len(text.split()))
        max_reasonable_duration = max(2.8, min(7.0, word_count * 0.85 + 1.5))
        if (end - start) > max_reasonable_duration:
            end = round(start + max_reasonable_duration, 2)

        # Prevent overlap with the next segment
        if idx + 1 < len(segments):
            next_start = float(segments[idx + 1]["start"])
            if end > next_start:
                end = max(round(start + 0.3, 2), round(next_start - 0.04, 2))

        cleaned.append({
            "id": len(cleaned),
            "start": round(start, 2),
            "end": round(end, 2),
            "text": text
        })

    return cleaned


def transcribe_single_audio_file(file_path: str) -> list[dict]:
    if not groq_client:
        raise RuntimeError("GROQ_API_KEY is not set.")
    whisper_models = ["whisper-large-v3-turbo", "whisper-large-v3"]
    transcription = None
    last_err = None
    for wm in whisper_models:
        try:
            with open(file_path, "rb") as f:
                transcription = groq_client.audio.transcriptions.create(
                    file=(file_path, f.read()),
                    model=wm,
                    prompt="Transcribe speech accurately with correct spelling and capitalization.",
                    response_format="verbose_json",
                    language="en",
                    temperature=0.0
                )
            break
        except Exception as e:
            last_err = e
            print(f"Whisper {wm} error: {e}", flush=True)
            err_str = str(e).lower()
            if "413" in err_str or "request_too_large" in err_str or "too large" in err_str:
                raise e

    if not transcription:
        if last_err:
            raise last_err
        raise RuntimeError("Groq Whisper transcription failed on all models.")

    raw_segments = getattr(transcription, "segments", []) or []
    clean_segments = []

    for seg in raw_segments:
        start = seg["start"] if isinstance(seg, dict) else seg.start
        end = seg["end"] if isinstance(seg, dict) else seg.end
        text = (seg["text"] if isinstance(seg, dict) else seg.text).strip()
        no_speech = (
            seg.get("no_speech_prob", 0.0)
            if isinstance(seg, dict)
            else getattr(seg, "no_speech_prob", 0.0)
        )

        if not text or no_speech > 0.65:
            continue

        clean_segments.append({
            "id": len(clean_segments),
            "start": round(float(start), 2),
            "end": round(float(end), 2),
            "text": text
        })

    return post_process_subtitle_timings(clean_segments)


def transcribe_audio_chunks(audio_path: str, duration: float = 0.0, chunk_sec: int = 600) -> list[dict]:
    """
    Slices long audio into 10-minute (600s) chunks using stream-copy (-c copy, 0s re-encode overhead)
    and transcribes them in parallel.
    """
    if duration <= 0:
        try:
            cmd = [
                "ffprobe", "-v", "error", "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1", audio_path
            ]
            out = subprocess.check_output(cmd, stderr=subprocess.DEVNULL).decode().strip()
            duration = float(out)
        except Exception:
            duration = 3600.0

    num_chunks = max(1, math.ceil(duration / chunk_sec))
    print(f"[WORKER] Chunking audio ({duration:.0f}s) into {num_chunks} chunks of {chunk_sec}s each...", flush=True)

    chunks_data = []
    temp_files = []
    timestamp = int(time.time())
    for i in range(num_chunks):
        offset = i * chunk_sec
        chunk_file = f"chunk_{i}_{timestamp}.mp3"
        temp_files.append(chunk_file)
        # Stream copy (-c copy) splits already-extracted MP3 instantaneously without re-encoding
        cmd = [
            "ffmpeg", "-y", "-nostdin",
            "-ss", str(offset), "-t", str(chunk_sec),
            "-i", audio_path,
            "-c", "copy",
            chunk_file
        ]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if os.path.exists(chunk_file) and os.path.getsize(chunk_file) > 500:
            chunks_data.append((i, offset, chunk_file))

    all_segments = []

    def transcribe_chunk_worker(item):
        idx, offset, cfile = item
        try:
            segs = transcribe_single_audio_file(cfile)
            adjusted = []
            for s in segs:
                adjusted.append({
                    "start": round(s["start"] + offset, 2),
                    "end": round(s["end"] + offset, 2),
                    "text": s["text"]
                })
            return (idx, adjusted)
        except Exception as err:
            print(f"[WORKER] Error in chunk {idx} (offset {offset}s): {err}", flush=True)
            return (idx, [])

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        results = list(executor.map(transcribe_chunk_worker, chunks_data))

    results.sort(key=lambda x: x[0])
    for _, segs in results:
        all_segments.extend(segs)

    for f in temp_files:
        try:
            if os.path.exists(f):
                os.remove(f)
        except Exception:
            pass

    all_segments.sort(key=lambda x: x["start"])
    return post_process_subtitle_timings(all_segments)


def transcribe_audio_fast(audio_path: str, duration: float = 0.0) -> list[dict]:
    if not groq_client:
        raise RuntimeError("GROQ_API_KEY is not set.")

    file_size = os.path.getsize(audio_path) if os.path.exists(audio_path) else 0
    if file_size > 22 * 1024 * 1024:
        print(f"[WORKER] Audio size {file_size / (1024*1024):.2f}MB exceeds 22MB. Chunking immediately...", flush=True)
        return transcribe_audio_chunks(audio_path, duration)

    try:
        return transcribe_single_audio_file(audio_path)
    except Exception as e:
        err_str = str(e).lower()
        if "413" in err_str or "request_too_large" in err_str or "too large" in err_str:
            print("[WORKER] Whisper 413 Entity Too Large. Chunking fallback triggered...", flush=True)
            return transcribe_audio_chunks(audio_path, duration)
        raise e


# ================= BULLETPROOF & ULTRA-FAST TRANSLATION ENGINE =================
LINE_PARSE_REGEX = re.compile(r"^\[?(\d+)\]?\s*(?:\|\|\||[:\-–—\.]|\))\s*(.+)$")


def has_persian_chars(text: str) -> bool:
    """Returns True if the string contains any Persian/Arabic script characters."""
    return bool(re.search(r"[\u0600-\u06FF]", text))


DIRECT_PHRASES = {
    "i'm coming": "داره میاد",
    "im coming": "داره میاد",
    "i am coming": "داره میاد",
    "i'm gonna cum": "داره میاد / دارم ارضا میشم",
    "im gonna cum": "داره میاد / دارم ارضا میشم",
    "i am gonna cum": "داره میاد / دارم ارضا میشم",
    "i'm about to cum": "داره میاد",
    "harder": "محکم‌تر",
    "harder baby": "محکم‌تر عزیزم",
    "go harder": "محکم‌تر بکن",
    "faster": "تندتر",
    "faster baby": "تندتر عزیزم",
    "go faster": "تندترش کن",
    "deeper": "تا تهش بکن",
    "go deeper": "تا تهش بکن",
    "fuck me": "بکن منو",
    "fuck me harder": "محکم‌تر بکن منو",
    "fuck yes": "آخ آره همینه",
    "fuck yeah": "آخ آره همینه",
    "oh fuck": "وای لعنتی",
    "oh yeah": "آخ آره",
    "oh yes": "آخ آره جونم",
    "yeah baby": "آره عشقم",
    "yes baby": "آره عشقم",
    "don't stop": "وای نستا",
    "dont stop": "وای نستا",
    "please don't stop": "تو رو خدا ول نکن",
    "do you like that": "خوشت میاد؟",
    "you like that": "حال می‌کنی؟",
    "you like that baby": "خوشت میاد عشقم؟",
    "do you like it": "خوشت میاد؟",
    "feels so good": "خیلی حال میده",
    "that feels so good": "وای چقدر حال میده",
    "it feels so good": "خیلی کیف میده",
    "suck it": "بخورش",
    "suck my dick": "ساک بزن برام",
    "eat my pussy": "کصمو بلیس",
    "look at me": "نگام کن",
    "look at you": "فدات بشم",
    "come here": "بیا اینجا",
    "take it off": "دربیارش",
    "take your clothes off": "لباساتو دربیار",
    "ride me": "بیا روم بشین",
    "ride it": "بشین روش",
    "good girl": "دختر قشنگم",
    "good boy": "پسر خوبم",
    "right there": "دقیقاً همونجا، آخ آره",
    "put it in": "بکنش تو",
    "give it to me": "بده به من، بکنش تو",
}


def clean_for_lookup(text: str) -> str:
    t = text.lower().strip()
    t = re.sub(r"[^\w\s']", "", t)
    return re.sub(r"\s+", " ", t).strip()


def match_direct_phrase(text: str) -> str:
    cleaned = clean_for_lookup(text)
    if cleaned in DIRECT_PHRASES:
        return DIRECT_PHRASES[cleaned]
    for k, v in DIRECT_PHRASES.items():
        if cleaned == k or (len(k) >= 8 and k in cleaned):
            return v
    return ""


def refine_colloquial_persian(text: str) -> str:
    """
    Polishes machine translations into authentic, natural colloquial Persian.
    Handles ZWNJ (\u200c) and converts bookish phrases into real-world spoken Persian.
    """
    if not text:
        return ""

    t = text.strip()

    replacements = [
        # "I'm coming / gonna cum" -> داره میاد
        (r"(?:من\s+)?دارم\s+می[\s\u200c]*آیم", "داره میاد"),
        (r"(?:من\s+)?دارم\s+میام", "داره میاد"),
        (r"(?:من\s+)?می[\s\u200c]*خوام\s+بیام", "داره آبم میاد"),
        (r"آبم\s+داره\s+میاد", "داره میاد"),

        # "Oh yeah / Yeah baby" -> آره جونم / آخ آره
        (r"اوه\s+بله", "آره جونم"),
        (r"اوه\s+آره", "آخ آره"),
        (r"بله\s+کودک", "آره عشقم"),
        (r"بله\s+عزیزم", "آره عشقم"),
        (r"اوه\s+خدای\s+من", "وای خدا"),

        # "Fuck / Fuck me / Fucking"
        (r"مرا\s+لعنت\s+کن", "بکن منو"),
        (r"لعنت\s+به\s+من", "بکن منو"),
        (r"مرا\s+بکن", "بکن منو"),
        (r"لعنتی\s+بله", "آخ آره همینه"),
        (r"لعنتی\s+خوب", "فوق‌العاده"),
        (r"لعنتی\s+داغ", "خیلی داغ و سکسی"),
        (r"لعنتی\s+عالی", "دیوونه‌کننده"),
        (r"فاک\s+می", "بکن منو"),
        (r"فاک", "لعنتی"),

        # "Harder / Faster / Deeper" -> محکم‌تر / تندتر / تا ته
        (r"سخت[\s\u200c]*تر", "محکم‌تر"),
        (r"سریع[\s\u200c]*تر", "تندتر"),
        (r"عمیق[\s\u200c]*تر", "تا ته"),
        (r"تا\s+انتها", "تا تهش"),

        # "Don't stop" -> وای نستا
        (r"متوقف\s+نشو", "وای نستا"),
        (r"توقف\s+نکن", "ول نکن"),
        (r"ایست\s+نکن", "ادامه بده"),

        # "You like that / You want it"
        (r"(?:آیا\s+)?(?:آن\s*را|اینو)\s+دوست\s+داری", "خوشت میاد؟"),
        (r"دوستش\s+داری\؟?", "حال می‌کنی؟"),
        (r"اینو\s+می[\s\u200c]*خواهی", "اینو می‌خوای؟"),

        # "So good / Feels so good / Taste good"
        (r"بسیار\s+خوب\s+است", "خیلی حال میده"),
        (r"احساس\s+خیلی\s+خوبی\s+دارد", "چقدر حال میده"),
        (r"خیلی\s+خوب\s+احساس\s+می[\s\u200c]*شه", "وای چه کیفی داره"),
        (r"مزه\s+بسیار\s+خوبی\s+دارد", "خیلی خوشمزه‌ست"),

        # "Look at me"
        (r"به\s+من\s+نگاه\s+کن(?:ید)?", "نگام کن"),

        # "Come here"
        (r"به\s+اینجا\s+بیا", "بیا اینجا"),
        (r"بیا\s+این[\s\u200c]*جا", "بیا اینجا"),

        # "Suck it / Lick it"
        (r"آن\s*را\s+مکیدن", "بخورش"),
        (r"مک\s+بزن", "بخورش"),
        (r"آن\s*را\s+لیس\s+بزن", "بلیسش"),
        (r"لیس\s+زدن", "لیسیدن"),

        # "Ride me / On top"
        (r"سوار\s+من\s+شو", "بیا روم بشین"),
        (r"سوار\s+شو", "بشین روم"),

        # "Take it off"
        (r"لباس‌هایت\s+را\s+درآور", "لباساتو دربیار"),
        (r"آن\s*را\s+درآور", "دربیارش"),

        # "Put it in / Push it in"
        (r"آن\s*را\s+قرار\s+دهید", "بکنش تو"),
        (r"بگذار\s+داخل", "بکنش تو"),
        (r"فشار\s+بده", "بکنش تو"),

        # Anatomy
        (r"گربه\s*(?:من|ات|ت)?", "کصم"),
        (r"خروس\s*(?:من|ات|ت)?", "کیرم"),
        (r"الاغ\s*(?:من|ات|ت)?", "کونم"),
        (r"سینه‌ها", "ممه‌ها"),
    ]

    for pattern, repl in replacements:
        t = re.sub(pattern, repl, t, flags=re.IGNORECASE)

    t = re.sub(r"\s+", " ", t).strip()
    return t


def parse_delimited_lines(raw_output: str) -> dict[int, str]:
    res_map = {}
    if not raw_output:
        return res_map

    for raw_line in raw_output.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("```"):
            continue
        if "|||" in line:
            parts = line.split("|||", 1)
            idx_str = re.sub(r"\D", "", parts[0])
            fa_text = parts[1].strip()
            if idx_str and fa_text and has_persian_chars(fa_text):
                res_map[int(idx_str)] = refine_colloquial_persian(fa_text)
        else:
            m = LINE_PARSE_REGEX.match(line)
            if m and has_persian_chars(m.group(2)):
                res_map[int(m.group(1))] = refine_colloquial_persian(m.group(2).strip())

    return res_map


SYSTEM_TRANSLATION_PROMPT = (
    "You are a master subtitle translator specializing in cinema, romantic drama, and intimate dialogue.\n"
    "Translate every numbered English line into completely natural, seductive, colloquial, real-world Iranian Persian (فارسی عامیانه، صمیمی و محاوره‌ای).\n"
    "CRITICAL RULES:\n"
    "1. Contextual Slang: Translate intimate dialogue naturally into real spoken Persian (e.g. 'I\\'m coming' -> 'داره میاد / دارم ارضا میشم', 'Harder' -> 'محکم‌تر', 'Don\\'t stop' -> 'وای نستا / ول نکن', 'Fuck me' -> 'بکن منو', 'Oh yeah' -> 'آخ آره جونم', 'You like that?' -> 'خوشت میاد؟', 'Suck it' -> 'بخورش').\n"
    "2. Natural Spoken Style: Never use bookish or literal machine phrases (NO 'سخت‌تر', NO 'من دارم می‌آیم', NO 'کودک').\n"
    "3. Format: Output ONLY: ID|||ترجمه روان فارسی"
)

GEMINI_SAFETY_OFF = [
    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
]


async def fast_gtx_single(client: httpx.AsyncClient, item: dict) -> tuple[int, str]:
    """Concurrent single-line GTX request."""
    url = "https://translate.googleapis.com/translate_a/single"
    params = {"client": "gtx", "sl": "en", "tl": "fa", "dt": "t", "q": item["text"]}
    try:
        r = await client.get(url, params=params, timeout=3.5)
        if r.status_code == 200:
            fa = r.json()[0][0][0]
            if fa and has_persian_chars(fa):
                return item["id"], refine_colloquial_persian(fa)
    except Exception:
        pass
    return item["id"], ""


async def try_fast_llm_batch(
    batch: list[dict],
    shared_client: httpx.AsyncClient | None = None,
    sem: asyncio.Semaphore | None = None
) -> dict[int, str]:
    """Ultra-fast LLM batch translation using shared connection pool + Gemini 2.5 Flash safety-unlocked fallback."""
    input_lines = "\n".join(f"{item['id']}|||{item['text']}" for item in batch)

    async def _run_with_client(client: httpx.AsyncClient) -> dict[int, str]:
        # 1. Try Groq fast models (llama-3.3-70b-versatile / llama-3.1-8b-instant)
        if GROQ_API_KEY:
            for groq_model in ("llama-3.1-8b-instant", "llama-3.3-70b-versatile"):
                try:
                    resp = await client.post(
                        "https://api.groq.com/openai/v1/chat/completions",
                        headers={"Authorization": f"Bearer {GROQ_API_KEY}"},
                        json={
                            "model": groq_model,
                            "messages": [
                                {"role": "system", "content": SYSTEM_TRANSLATION_PROMPT},
                                {"role": "user", "content": input_lines}
                            ],
                            "temperature": 0.2
                        },
                        timeout=6.0
                    )
                    if resp.status_code == 200:
                        raw_text = resp.json()["choices"][0]["message"]["content"]
                        raw_text = re.sub(r"<think>.*?</think>", "", raw_text, flags=re.DOTALL).strip()
                        parsed = parse_delimited_lines(raw_text)
                        if len(parsed) >= int(len(batch) * 0.7):
                            return parsed
                except Exception:
                    pass

        # 2. Try Gemini 2.5 Flash / 2.0 Flash with BLOCK_NONE safety settings
        if GEMINI_API_KEY:
            for gemini_model in ("gemini-2.5-flash", "gemini-2.0-flash"):
                try:
                    url = f"https://generativelanguage.googleapis.com/v1beta/models/{gemini_model}:generateContent?key={GEMINI_API_KEY}"
                    payload = {
                        "contents": [{
                            "parts": [{"text": f"{SYSTEM_TRANSLATION_PROMPT}\n\nEnglish Subtitles:\n{input_lines}"}]
                        }],
                        "safetySettings": GEMINI_SAFETY_OFF,
                        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 2048}
                    }
                    resp = await client.post(url, json=payload, timeout=6.5)
                    if resp.status_code == 200:
                        candidates = resp.json().get("candidates", [])
                        if candidates:
                            parts = candidates[0].get("content", {}).get("parts", [])
                            if parts and "text" in parts[0]:
                                parsed = parse_delimited_lines(parts[0]["text"])
                                if len(parsed) >= int(len(batch) * 0.7):
                                    return parsed
                except Exception:
                    pass

        return {}

    if sem:
        async with sem:
            if shared_client:
                return await _run_with_client(shared_client)
            async with httpx.AsyncClient(timeout=7.0) as temp_client:
                return await _run_with_client(temp_client)
    else:
        if shared_client:
            return await _run_with_client(shared_client)
        async with httpx.AsyncClient(timeout=7.0) as temp_client:
            return await _run_with_client(temp_client)


async def translate_all_subtitles_instant(segments: list[dict]) -> list[dict]:
    """
    Guaranteed ultra-fast subtitle translation:
    1. Direct phrase matching for instant colloquial translation (0ms).
    2. Parallel LLM batching over a shared connection pool.
    3. Instant parallel GTX batch + concurrent single-line fallback.
    4. Post-refinement to guarantee 100% natural, unfiltered colloquial Persian.
    """
    if not segments:
        return []

    combined_map = {}

    # Step 0: Instant Direct Phrase Matching (0 ms)
    remaining_segments = []
    for item in segments:
        direct = match_direct_phrase(item["text"])
        if direct:
            combined_map[item["id"]] = direct
        else:
            remaining_segments.append(item)

    async with httpx.AsyncClient(timeout=7.0) as shared_client:
        # Step 1: Fast Parallel LLM on remaining segments (chunk size 15, max 4 concurrent requests)
        if remaining_segments:
            batch_size = 15
            sem = asyncio.Semaphore(4)
            batches = [remaining_segments[i:i + batch_size] for i in range(0, len(remaining_segments), batch_size)]
            try:
                tasks = [try_fast_llm_batch(b, shared_client=shared_client, sem=sem) for b in batches]
                results = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=12.0)
                for r in results:
                    if isinstance(r, dict):
                        combined_map.update(r)
            except Exception:
                pass

        # Step 2: Instant Google GTX fallback for any missing lines
        missing = [
            item for item in segments
            if item["id"] not in combined_map or not has_persian_chars(combined_map.get(item["id"], ""))
        ]

        if missing:
            try:
                texts = [item["text"].replace("\n", " ").strip() for item in missing]
                joined = "\n".join(texts)
                r = await shared_client.get(
                    "https://translate.googleapis.com/translate_a/single",
                    params={"client": "gtx", "sl": "en", "tl": "fa", "dt": "t", "q": joined},
                    timeout=4.5
                )
                if r.status_code == 200:
                    data = r.json()
                    translated_full = "".join(seg[0] for seg in data[0] if seg and seg[0])
                    lines = [l.strip() for l in translated_full.splitlines() if l.strip()]
                    if len(lines) == len(missing):
                        for item, l in zip(missing, lines):
                            if l and has_persian_chars(l):
                                combined_map[item["id"]] = refine_colloquial_persian(l)

                still_missing = [
                    item for item in missing
                    if item["id"] not in combined_map or not has_persian_chars(combined_map.get(item["id"], ""))
                ]
                if still_missing:
                    gtx_tasks = [fast_gtx_single(shared_client, it) for it in still_missing]
                    single_results = await asyncio.gather(*gtx_tasks, return_exceptions=True)
                    for res in single_results:
                        if isinstance(res, tuple) and res[1]:
                            combined_map[res[0]] = res[1]
            except Exception as e:
                print(f"GTX fallback note: {e}", flush=True)

    # Step 3: Fast translate_batch fallback from deep_translator if anything is still missing
    still_missing = [
        item for item in segments
        if item["id"] not in combined_map or not has_persian_chars(combined_map.get(item["id"], ""))
    ]
    if still_missing:
        try:
            translator = GoogleTranslator(source="en", target="fa")
            texts_to_trans = [it["text"] for it in still_missing]
            loop = asyncio.get_running_loop()
            batch_res = await loop.run_in_executor(None, translator.translate_batch, texts_to_trans)
            for it, tr in zip(still_missing, batch_res):
                if tr and has_persian_chars(tr):
                    combined_map[it["id"]] = refine_colloquial_persian(tr)
        except Exception:
            pass

    # Assemble final subtitles with thorough Persian polishing
    translated_all = []
    for item in segments:
        cid = item["id"]
        fa_text = combined_map.get(cid, "").strip()
        if not fa_text:
            fa_text = item["text"]
        else:
            fa_text = refine_colloquial_persian(fa_text)

        translated_all.append({
            "start": item["start"],
            "end": item["end"],
            "text": fa_text
        })

    return translated_all


# ================= SUBTITLE (.ASS) & ULTRAFAST HIGH-QUALITY FFMPEG =================
def format_ass_time(seconds: float) -> str:
    hrs = int(seconds // 3600)
    mins = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    centis = int(round((seconds - int(seconds)) * 100))
    if centis >= 100:
        centis = 99
    return f"{hrs}:{mins:02d}:{secs:02d}.{centis:02d}"


def generate_ass_file(subtitles: list[dict], user_tag: str, width: int, height: int, ass_path: str):
    min_dim = min(width, height)
    sub_font_size = max(24, int(min_dim * 0.046))
    tag_font_size = max(18, int(min_dim * 0.032))
    sub_margin_v = int(height * 0.085)
    tag_margin_v = int(height * 0.022)
    outline_size = max(2, int(sub_font_size * 0.08))

    ass_header = f"""[Script Info]
ScriptType: v4.00+
PlayResX: {width}
PlayResY: {height}
WrapStyle: 0
ScaledBorderAndShadow: yes

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: YellowSub,Vazirmatn,{sub_font_size},&H0000FFFF,&H000000FF,&H00000000,&H80000000,-1,0,0,0,100,100,0,0,1,{outline_size},1,2,40,40,{sub_margin_v},1
Style: BottomTag,Vazirmatn,{tag_font_size},&H30FFFFFF,&H000000FF,&H00000000,&H60000000,-1,0,0,0,100,100,0,0,1,2,1,2,20,20,{tag_margin_v},1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""
    lines = [ass_header.strip()]

    clean_tag = user_tag.replace("\n", " ").replace("{", "").replace("}", "")
    lines.append(f"Dialogue: 0,0:00:00.00,9:59:59.00,BottomTag,,0,0,0,,{clean_tag}")

    for sub in subtitles:
        start_str = format_ass_time(sub["start"])
        end_str = format_ass_time(sub["end"])
        clean_text = sub["text"].replace("\n", "\\N").replace("{", "").replace("}", "")
        rtl_text = f"\u202B{clean_text}"
        lines.append(f"Dialogue: 1,{start_str},{end_str},YellowSub,,0,0,0,,{rtl_text}")

    with open(ass_path, "w", encoding="utf-8-sig") as f:
        f.write("\n".join(lines) + "\n")


async def burn_subtitles_ffmpeg_size_capped(
    client: TelegramClient,
    input_video: str,
    ass_path: str,
    fonts_dir: str,
    output_video: str,
    target_w: int,
    target_h: int,
    orig_w: int,
    orig_h: int,
    total_duration: int,
    orig_fps: float,
    orig_bitrate_kbps: int
):
    fps_filter = "fps=30," if orig_fps > 30.5 else ""
    scale_filter = f"scale={target_w}:{target_h}:flags=fast_bilinear," if (target_w != orig_w or target_h != orig_h) else ""

    escaped_ass = ass_path.replace("\\", "/").replace(":", r"\:")
    escaped_fonts = fonts_dir.replace("\\", "/").replace(":", r"\:")
    vf_filter = f"{fps_filter}{scale_filter}ass={escaped_ass}:fontsdir={escaped_fonts}"

    is_1080p_or_higher = max(target_w, target_h) >= 1920 or min(target_w, target_h) >= 1080
    if is_1080p_or_higher:
        target_max_kbps = min(orig_bitrate_kbps, 6000)
        target_max_kbps = max(2200, target_max_kbps)
        crf_val = "23"
    else:
        target_max_kbps = min(orig_bitrate_kbps, 3000)
        target_max_kbps = max(800, target_max_kbps)
        crf_val = "24"

    bufsize_kbps = target_max_kbps * 2

    def _build_ffmpeg_cmd(audio_codec_args: list[str]) -> list[str]:
        return [
            "ffmpeg", "-y", "-nostdin",
            "-threads", "0",
            "-thread_type", "slice+frame",
            "-i", input_video,
            "-vf", vf_filter,
            "-c:v", "libx264",
            "-preset", "ultrafast",
            "-tune", "fastdecode",
            "-crf", crf_val,
            "-maxrate", f"{target_max_kbps}k",
            "-bufsize", f"{bufsize_kbps}k",
            "-pix_fmt", "yuv420p",
            *audio_codec_args,
            "-movflags", "+faststart",
            "-progress", "pipe:1",
            "-nostats",
            output_video
        ]

    # First try stream-copying audio (-c:a copy) for maximum speed; fallback to AAC if container rejects codec
    for attempt_idx, audio_args in enumerate((["-c:a", "copy"], ["-c:a", "aac", "-b:a", "128k"])):
        cmd = _build_ffmpeg_cmd(audio_args)
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        while True:
            line = await process.stdout.readline()
            if not line:
                break
            line_str = line.decode("utf-8", errors="ignore").strip()
            if line_str.startswith("out_time_ms=") and total_duration > 0:
                try:
                    ms = int(line_str.split("=")[1])
                    current_sec = ms / 1_000_000.0
                    pct = min(99.0, max(0.0, (current_sec / total_duration) * 100.0))
                    bar = make_progress_bar(pct)
                    await update_status(
                        client,
                        f"🎬 <b>مرحله ۴ از ۵:</b> رندر توربو زیرنویس...\n"
                        f"{bar} <b>{pct:.1f}%</b>",
                        force=False
                    )
                except Exception:
                    pass

        await process.wait()
        if process.returncode == 0:
            return
        if attempt_idx == 1:
            stderr_out = (await process.stderr.read()).decode("utf-8", errors="ignore")
            raise RuntimeError(f"FFmpeg failed: {stderr_out[-300:]}")
        print("[WORKER] FFmpeg -c:a copy incompatible with source audio, retrying with AAC...", flush=True)


# ================= RESILIENT MTPROTO CLIENT STARTUP =================
async def start_bot_client_safely(session_path: str) -> TelegramClient:
    """
    Starts the Telethon client using the cached session.
    If the cached session is locked, duplicated, or invalidated, automatically purges it and logs in cleanly.
    """
    for attempt in range(2):
        client = TelegramClient(
            session_path,
            API_ID,
            API_HASH,
            flood_sleep_threshold=60
        )
        try:
            await client.start(bot_token=BOT_TOKEN)
            return client
        except (AuthKeyDuplicatedError, AuthKeyUnregisteredError, AuthKeyError, Exception) as e:
            try:
                await client.disconnect()
            except Exception:
                pass
            if attempt == 0:
                print(f"[WORKER] Cached session issue ({e}), resetting session file...", flush=True)
                for f in glob.glob(f"{session_path}*"):
                    try:
                        os.remove(f)
                    except Exception:
                        pass
            else:
                raise e


# ================= MAIN PIPELINE =================
async def main():
    os.makedirs("session_cache", exist_ok=True)
    token_hash = hashlib.md5(BOT_TOKEN.encode()).hexdigest()[:10]
    session_path = f"session_cache/bot_{token_hash}"

    main_client = await start_bot_client_safely(session_path)

    try:
        async def dl_progress(current, total):
            if total > 0:
                pct = min(100.0, (current / total) * 100.0)
                mb_cur = current / (1024 * 1024)
                mb_tot = total / (1024 * 1024)
                bar = make_progress_bar(pct)
                text = (
                    f"📥 <b>مرحله ۱ از ۵:</b> دانلود ۶ کاناله توربو...\n"
                    f"{bar} <b>{pct:.1f}%</b> (<code>{mb_cur:.1f} / {mb_tot:.1f} MB</code>)"
                )
                await update_status(main_client, text, force=False)

        # 1. 6-Socket Striped Download
        await update_status(main_client, "📥 <b>مرحله ۱ از ۵:</b> شروع دانلود موازی ۶ کاناله...", force=True)
        video_msg = await main_client.get_messages(CHAT_ID, ids=VIDEO_MSG_ID)
        if not video_msg:
            raise RuntimeError("ویدیوی مورد نظر یافت نشد (ممکن است حذف شده باشد).")

        input_video = "input_video.mp4"
        await striped_download_file(main_client, video_msg, input_video, progress_callback=dl_progress, pool_size=6)

        thumb_path = None
        if THUMB_MSG_ID and THUMB_MSG_ID > 0:
            thumb_msg = await main_client.get_messages(CHAT_ID, ids=THUMB_MSG_ID)
            if thumb_msg and thumb_msg.photo:
                thumb_path = await main_client.download_media(thumb_msg, file="custom_thumb.jpg")

        orig_w, orig_h, duration, orig_fps, orig_bitrate_kbps = get_video_info(input_video)
        target_w, target_h = compute_target_resolution(orig_w, orig_h)
        print(
            f"[WORKER] Video info: {orig_w}x{orig_h} -> target: {target_w}x{target_h}, "
            f"duration: {duration}s, fps: {orig_fps:.2f}, bitrate: {orig_bitrate_kbps}k",
            flush=True
        )

        # 2. Extract Fast 16kHz MP3 Audio & Transcribe with Groq Whisper Turbo
        await update_status(main_client, "🎙 <b>مرحله ۲ از ۵:</b> تشخیص سریع گفتار (Whisper Turbo)...", force=True)
        audio_path = "extracted_audio.mp3"
        t0_audio = time.time()
        extract_ultrafast_audio(input_video, audio_path)
        print(f"[WORKER] Audio extracted in {time.time() - t0_audio:.2f}s, sending to Whisper...", flush=True)
        t0_whisper = time.time()
        segments = transcribe_audio_fast(audio_path, duration=duration)
        print(f"[WORKER] Whisper recognized {len(segments)} segments in {time.time() - t0_whisper:.2f}s", flush=True)

        if not segments:
            await update_status(main_client, "⚠️ گفتگوی انگلیسی تشخیص داده نشد، در حال درج تگ...", force=True)
            translated_subs = []
        else:
            # 3. Guaranteed Ultra-Fast Subtitle Translation (1-3 seconds)
            await update_status(
                main_client,
                f"🧠 <b>مرحله ۳ از ۵:</b> ترجمه آنی ({len(segments)} خط)...",
                force=True
            )
            t0_trans = time.time()
            translated_subs = await translate_all_subtitles_instant(segments)
            print(f"[WORKER] Translation completed for {len(translated_subs)} lines in {time.time() - t0_trans:.2f}s", flush=True)

        # 4. Generate ASS & Burn with Ultrafast FFmpeg
        await update_status(main_client, "🎬 <b>مرحله ۴ از ۵:</b> شروع رندر توربو زیرنویس...", force=True)
        ass_path = "subtitles.ass"
        fonts_dir = "./fonts"
        output_video = "output_subtitled.mp4"

        generate_ass_file(translated_subs, USER_TAG, target_w, target_h, ass_path)
        print(f"[WORKER] Starting FFmpeg burning (ultrafast, {target_w}x{target_h})...", flush=True)
        t0_burn = time.time()
        await burn_subtitles_ffmpeg_size_capped(
            main_client,
            input_video,
            ass_path,
            fonts_dir,
            output_video,
            target_w,
            target_h,
            orig_w,
            orig_h,
            duration,
            orig_fps,
            orig_bitrate_kbps
        )
        print(f"[WORKER] FFmpeg finished burning in {time.time() - t0_burn:.2f}s", flush=True)

        if not thumb_path:
            thumb_path = "auto_thumb.jpg"
            subprocess.run([
                "ffmpeg", "-y", "-nostdin", "-ss", "00:00:01", "-i", output_video,
                "-vframes", "1", "-q:v", "2", thumb_path
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if not os.path.exists(thumb_path):
                thumb_path = None

        # 5. 6-Socket Striped Upload & Streaming Delivery
        out_size_mb = os.path.getsize(output_video) / (1024 * 1024)
        await update_status(
            main_client,
            f"📤 <b>مرحله ۵ از ۵:</b> آپلود ۶ کاناله (<code>{out_size_mb:.1f} MB</code>)...",
            force=True
        )

        async def ul_progress(current, total):
            if total > 0:
                pct = min(100.0, (current / total) * 100.0)
                mb_cur = current / (1024 * 1024)
                mb_tot = total / (1024 * 1024)
                bar = make_progress_bar(pct)
                text = (
                    f"📤 <b>مرحله ۵ از ۵:</b> آپلود توربو ویدیوی نهایی...\n"
                    f"{bar} <b>{pct:.1f}%</b> (<code>{mb_cur:.1f} / {mb_tot:.1f} MB</code>)"
                )
                await update_status(main_client, text, force=False)

        uploaded_input_file = await striped_upload_file(
            main_client, output_video, progress_callback=ul_progress, pool_size=6
        )

        safe_tag = USER_TAG.replace("<", "").replace(">", "")
        await main_client.send_file(
            CHAT_ID,
            file=uploaded_input_file,
            thumb=thumb_path,
            caption=f"✅ <b>زیرنویس فارسی با موفقیت اضافه شد!</b>\n🏷 <code>{safe_tag}</code>",
            parse_mode="html",
            supports_streaming=True,
            reply_to=VIDEO_MSG_ID,
            attributes=[
                DocumentAttributeVideo(
                    duration=duration,
                    w=target_w,
                    h=target_h,
                    supports_streaming=True
                )
            ]
        )

        try:
            await main_client.delete_messages(CHAT_ID, STATUS_MSG_ID)
        except Exception:
            pass

    except Exception as e:
        error_msg = (
            f"❌ <b>متاسفانه خطایی در حین پردازش رخ داد:</b>\n"
            f"<code>{str(e)[:300]}</code>"
        )
        try:
            await main_client.edit_message(CHAT_ID, STATUS_MSG_ID, error_msg, parse_mode="html")
        except Exception:
            pass
        raise e
    finally:
        await main_client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
