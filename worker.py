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
RAW_USER_TAG = (os.getenv("USER_TAG") or "@YourChannelTag").strip()


def unpack_user_tag_and_options(raw_tag: str) -> tuple[str, dict]:
    opts = {"color": "yellow", "size": "normal", "tag_pos": "bottom", "tone": "uncensored"}
    if "__OPT__" not in raw_tag:
        return raw_tag, opts
    clean_tag, opt_part = raw_tag.split("__OPT__", 1)
    parts = [p.strip() for p in opt_part.split(",")]
    if len(parts) >= 1 and parts[0]:
        opts["color"] = parts[0]
    if len(parts) >= 2 and parts[1]:
        opts["size"] = parts[1]
    if len(parts) >= 3 and parts[2]:
        opts["tag_pos"] = parts[2]
    if len(parts) >= 4 and parts[3]:
        opts["tone"] = parts[3]
    return (clean_tag.strip() or "@YourChannelTag"), opts


USER_TAG, RENDER_OPTS = unpack_user_tag_and_options(RAW_USER_TAG)

groq_client = Groq(api_key=GROQ_API_KEY, max_retries=1, timeout=30.0) if GROQ_API_KEY else None
last_edit_time = 0.0


# ================= PORTABLE I/O HELPERS =================
def safe_pread(fd: int, size: int, offset: int) -> bytes:
    if hasattr(os, "pread"):
        return os.pread(fd, size, offset)
    pos = os.lseek(fd, 0, os.SEEK_CUR)
    os.lseek(fd, offset, os.SEEK_SET)
    data = os.read(fd, size)
    os.lseek(fd, pos, os.SEEK_SET)
    return data


def safe_pwrite(fd: int, data: bytes, offset: int):
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
        asyncio.create_task(_do_edit_status(client, step_text))


def make_progress_bar(percent: float) -> str:
    filled = max(0, min(10, int(round(percent / 10))))
    return "🟩" * filled + "⬜" * (10 - filled)


def format_duration_fa(seconds: float) -> str:
    total_sec = max(0.1, float(seconds))
    if total_sec < 60:
        return f"{total_sec:.1f} ثانیه"
    mins = int(total_sec // 60)
    secs = int(round(total_sec % 60))
    if secs == 60:
        mins += 1
        secs = 0
    return f"{mins} دقیقه و {secs} ثانیه" if secs > 0 else f"{mins} دقیقه"


# ================= FLOOD-FREE 6-SOCKET MTPROTO SENDER ENGINE =================
async def create_sender_pool(client: TelegramClient, dc_id: int, pool_size: int = 6) -> list[MTProtoSender]:
    dc = await client._get_dc(dc_id)
    senders = []
    if dc_id == client.session.dc_id:
        for _ in range(pool_size):
            sender = MTProtoSender(client.session.auth_key, loggers=client._log)
            await sender.connect(
                client._connection(dc.ip_address, dc.port, dc.id, loggers=client._log, proxy=client._proxy)
            )
            senders.append(sender)
    else:
        first_sender = await client._borrow_exported_sender(dc_id)
        shared_auth_key = first_sender.auth_key
        await client._return_exported_sender(first_sender)
        for _ in range(pool_size):
            sender = MTProtoSender(shared_auth_key, loggers=client._log)
            await sender.connect(
                client._connection(dc.ip_address, dc.port, dc.id, loggers=client._log, proxy=client._proxy)
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
    part_size = 512 * 1024
    total_parts = math.ceil(file_size / part_size)
    dc_id = doc.dc_id

    location = InputDocumentFileLocation(
        id=doc.id, access_hash=doc.access_hash, file_reference=doc.file_reference, thumb_size=""
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
                        sender, GetFileRequest(location=location, offset=offset, limit=part_size)
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
                        id=new_doc.id, access_hash=new_doc.access_hash, file_reference=new_doc.file_reference, thumb_size=""
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
    part_size = 512 * 1024
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
                            file_id=file_id, file_part=part_idx, file_total_parts=total_parts, bytes=chunk
                        )
                    else:
                        req = SaveFilePartRequest(file_id=file_id, file_part=part_idx, bytes=chunk)
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
    return InputFile(id=file_id, parts=total_parts, name=file_name, md5_checksum=hash_md5.hexdigest())


# ================= VIDEO & AUDIO PROCESSING =================
def get_video_info(video_path: str) -> tuple[int, int, int, float, int]:
    cmd = [
        "ffprobe", "-v", "error", "-select_streams", "v:0",
        "-show_entries", "stream=width,height,r_frame_rate,bit_rate,tags,side_data_list:format=duration,bit_rate",
        "-of", "json", video_path
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    data = json.loads(res.stdout)
    stream = data["streams"][0]
    fmt = data.get("format", {})

    width = int(stream.get("width", 1920))
    height = int(stream.get("height", 1080))

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
    min_dim = min(width, height)
    if min_dim <= 1080:
        return (width - (width % 2), height - (height % 2))
    scale_factor = 1080.0 / float(min_dim)
    new_w = int(round(width * scale_factor))
    new_h = int(round(height * scale_factor))
    return (new_w - (new_w % 2), new_h - (new_h % 2))


def extract_ultrafast_audio(video_path: str, audio_path: str):
    cmd = [
        "ffmpeg", "-y", "-nostdin", "-threads", "0", "-i", video_path,
        "-vn", "-sn", "-ac", "1", "-ar", "16000",
        "-c:a", "libmp3lame", "-b:a", "32k", "-q:a", "9", audio_path
    ]
    res = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    if res.returncode != 0:
        cmd_fallback = [
            "ffmpeg", "-y", "-nostdin", "-threads", "0", "-i", video_path,
            "-vn", "-sn", "-ac", "1", "-ar", "16000", "-c:a", "aac", "-b:a", "32k", audio_path
        ]
        subprocess.run(cmd_fallback, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def post_process_subtitle_timings(segments: list[dict]) -> list[dict]:
    if not segments:
        return []
    cleaned = []
    for idx, seg in enumerate(segments):
        start = float(seg["start"])
        end = float(seg["end"])
        text = seg["text"].strip()
        if not text or end <= start:
            continue

        word_count = max(1, len(text.split()))
        max_reasonable_duration = max(2.8, min(7.0, word_count * 0.85 + 1.5))
        if (end - start) > max_reasonable_duration:
            end = round(start + max_reasonable_duration, 2)

        if idx + 1 < len(segments):
            next_start = float(segments[idx + 1]["start"])
            if end > next_start:
                end = max(round(start + 0.3, 2), round(next_start - 0.04, 2))

        cleaned.append({"id": len(cleaned), "start": round(start, 2), "end": round(end, 2), "text": text})
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
            if any(k in str(e).lower() for k in ("413", "request_too_large", "too large")):
                raise e

    if not transcription:
        raise last_err or RuntimeError("Groq Whisper transcription failed.")

    raw_segments = getattr(transcription, "segments", []) or []
    clean_segments = []
    for seg in raw_segments:
        start = seg["start"] if isinstance(seg, dict) else seg.start
        end = seg["end"] if isinstance(seg, dict) else seg.end
        text = (seg["text"] if isinstance(seg, dict) else seg.text).strip()
        no_speech = seg.get("no_speech_prob", 0.0) if isinstance(seg, dict) else getattr(seg, "no_speech_prob", 0.0)
        if not text or no_speech > 0.65:
            continue
        clean_segments.append({"id": len(clean_segments), "start": round(float(start), 2), "end": round(float(end), 2), "text": text})

    return post_process_subtitle_timings(clean_segments)


def transcribe_audio_chunks(audio_path: str, duration: float = 0.0, chunk_sec: int = 600) -> list[dict]:
    if duration <= 0:
        try:
            out = subprocess.check_output(
                ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", audio_path],
                stderr=subprocess.DEVNULL
            ).decode().strip()
            duration = float(out)
        except Exception:
            duration = 3600.0

    num_chunks = max(1, math.ceil(duration / chunk_sec))
    chunks_data, temp_files = [], []
    timestamp = int(time.time())
    for i in range(num_chunks):
        offset = i * chunk_sec
        chunk_file = f"chunk_{i}_{timestamp}.mp3"
        temp_files.append(chunk_file)
        subprocess.run(
            ["ffmpeg", "-y", "-nostdin", "-ss", str(offset), "-t", str(chunk_sec), "-i", audio_path, "-c", "copy", chunk_file],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        if os.path.exists(chunk_file) and os.path.getsize(chunk_file) > 500:
            chunks_data.append((i, offset, chunk_file))

    def transcribe_chunk_worker(item):
        idx, offset, cfile = item
        try:
            segs = transcribe_single_audio_file(cfile)
            return (idx, [{"start": round(s["start"] + offset, 2), "end": round(s["end"] + offset, 2), "text": s["text"]} for s in segs])
        except Exception as err:
            print(f"[WORKER] Error in chunk {idx}: {err}", flush=True)
            return (idx, [])

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        results = list(executor.map(transcribe_chunk_worker, chunks_data))

    results.sort(key=lambda x: x[0])
    all_segments = [s for _, segs in results for s in segs]

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
        return transcribe_audio_chunks(audio_path, duration)
    try:
        return transcribe_single_audio_file(audio_path)
    except Exception as e:
        if any(k in str(e).lower() for k in ("413", "request_too_large", "too large")):
            return transcribe_audio_chunks(audio_path, duration)
        raise e


# ================= ZERO-LATENCY EROTIC & COLLOQUIAL TRANSLATION ENGINE =================
LINE_PARSE_REGEX = re.compile(r"^\[?(\d+)\]?\s*(?:\|\|\||[:\-–—\.]|\))\s*(.+)$")
PERSIAN_CHAR_REGEX = re.compile(r"[\u0600-\u06FF]")
CLEAN_PUNCT_REGEX = re.compile(r"[^\w\s']")
MULTI_SPACE_REGEX = re.compile(r"\s+")


def has_persian_chars(text: str) -> bool:
    return bool(PERSIAN_CHAR_REGEX.search(text))


# O(1) Hash-Map of 185+ Erotic & Colloquial Dialogues (< 0.0005s execution time)
DIRECT_PHRASES = {
    "i'm coming": "داره میاد", "im coming": "داره میاد", "i am coming": "داره میاد",
    "i'm cumming": "دارم ارضا میشم، داره میاد", "im cumming": "دارم ارضا میشم، داره میاد",
    "i'm gonna cum": "داره میاد / دارم ارضا میشم", "im gonna cum": "داره میاد / دارم ارضا میشم",
    "i am gonna cum": "داره میاد / دارم ارضا میشم", "i'm going to cum": "دارم ارضا میشم",
    "i'm about to cum": "داره آبم میاد", "im about to cum": "داره آبم میاد",
    "i'm so close": "دیگه داره میاد، خیلی نزدیکم", "im so close": "دیگه داره میاد، خیلی نزدیکم",
    "are you close": "آبت داره میاد؟", "are you gonna cum": "داری ارضا میشی؟",
    "cum for me": "برام ارضا شو", "cum for me baby": "برام ارضا شو عشقم",
    "cum inside me": "آبشو بریز توش", "cum in me": "بریز توش",
    "cum on my face": "بریز رو صورتم", "cum on my tits": "آبتو بریز رو ممه‌هام",
    "cum on my ass": "آبتو بریز رو کونم", "cum in my mouth": "بریز تو دهنم",
    "fill me up": "توشو پر از آب کن", "breed me": "آبشو تا قطره آخر بریز توش",
    "shoot your load": "آبتو بپاش", "give me your cum": "آبتو بده به من",
    "swallow it": "قورتش بده", "swallow my cum": "آبمو قورت بده",
    "don't cum yet": "هنوز ارضا نشو", "dont cum yet": "هنوز ارضا نشو",
    "make me cum": "کاری کن ارضا بشم", "you make me cum": "تو منو به اوج میرسونی",
    "let's cum together": "بیا با هم ارضا بشیم",
    "harder": "محکم‌تر", "harder baby": "محکم‌تر عزیزم", "harder please": "تو رو خدا محکم‌تر",
    "go harder": "محکم‌تر بکن", "fuck me harder": "محکم‌تر بکن منو",
    "faster": "تندتر", "faster baby": "تندتر عزیزم", "go faster": "تندترش کن",
    "a little faster": "یکم تندترش کن", "deeper": "تا تهش بکن", "go deeper": "تا تهش فشار بده",
    "deeper baby": "تا تهش بکن عشقم", "all the way in": "تا دسته بکنش تو", "balls deep": "تا تهِ تهش",
    "slower": "آروم‌تر", "go slower": "آروم‌تر بکن", "nice and slow": "نرم و آروم",
    "don't stop": "وای نستا", "dont stop": "وای نستا", "please don't stop": "تو رو خدا ول نکن",
    "please dont stop": "تو رو خدا ول نکن", "never stop": "اصلاً ولش نکن",
    "keep going": "ادامه بده، قطعش نکن", "keep doing that": "همینجوری ادامه بده",
    "don't pull out": "درش نیار", "dont pull out": "درش نیار", "keep it inside": "نگهش دار توش",
    "just like that": "دقیقاً همونطوری، عالیه", "right there": "دقیقاً همونجا، آخ آره",
    "right there baby": "دقیقاً همونجا عشقم", "hit that spot": "بزن به همون نقطه",
    "fuck me": "بکن منو", "fuck me baby": "بکن منو عشقم", "fuck me like that": "همینجوری منو بکن",
    "fuck yes": "آخ آره همینه", "fuck yeah": "آخ آره همینه", "hell yeah": "آره لعنتی همینه",
    "oh fuck": "وای لعنتی", "oh fuck yeah": "وای آره لعنتی", "holy fuck": "وای خدا چقدر خوبه",
    "oh god": "آخ خدایا", "oh my god": "وای خدای من", "oh my fucking god": "وای خدا دارم دیوونه میشم",
    "oh yeah": "آخ آره", "oh yeah baby": "آخ آره جونم", "oh yes": "آخ آره جونم",
    "yes yes yes": "آره آره آره!", "yeah baby": "آره عشقم", "yes baby": "آره عشقم",
    "yes daddy": "آره ددی جونم", "oh daddy": "آخ ددی جون",
    "feels so good": "خیلی حال میده", "that feels so good": "وای چقدر حال میده",
    "it feels so good": "خیلی کیف میده", "you feel so good": "حس کردنت فوق‌العاده‌ست",
    "this feels amazing": "حسش دیوونه‌کننده‌ست", "so fucking good": "لعنتی چقدر حال میده",
    "it hurts so good": "دردِ شیرینی داره، خیلی خوبه",
    "do you like that": "خوشت میاد؟", "you like that": "حال می‌کنی؟",
    "you like that baby": "خوشت میاد عشقم؟", "do you like it": "خوشت میاد؟",
    "how does that feel": "حسش چطوره؟ حال میده؟", "you like how that feels": "از حسش خوشت میاد؟",
    "you want this": "اینو می‌خوای؟", "do you want it": "می‌خوایش؟",
    "tell me what you want": "بگو چی می‌خوای برات بکنم", "beg for it": "التماس کن تا بهت بدم",
    "say my name": "اسممو صدا کن", "who's your daddy": "بگو کی صاحبته؟", "whos your daddy": "بگو کی صاحبته؟",
    "you're mine": "تو مال منی", "you are so hot": "خیلی داغ و سکسی هستی",
    "you're so sexy": "چقدر تو جذابی", "you're so tight": "چقدر کصت تنگه",
    "so fucking tight": "لعنتی چقدر تنگ و داغه", "you're so wet": "چقدر خیس شدی",
    "i'm so wet": "حسابی خیس شدم", "im so wet": "حسابی خیس شدم",
    "my pussy is so wet": "کصم خیسِ آب شده", "i'm so horny": "خیلی حشریم", "im so horny": "خیلی حشریم",
    "you make me so horny": "تو منو حسابی حشری می‌کنی",
    "it's so big": "وای چقدر بزرگ و کلفته", "its so big": "وای چقدر بزرگ و کلفته",
    "your dick is so big": "کیرت خیلی بزرگه", "your cock is huge": "کیرت واقعاً گنده‌ست",
    "good girl": "دختر قشنگم", "such a good girl": "چه دختر حرف‌گوش‌کنی",
    "bad girl": "دختر شیطون", "naughty girl": "دختر حشری و شیطون", "good boy": "پسر خوبم",
    "suck it": "بخورش", "suck it baby": "بخورش عزیزم", "suck my dick": "ساک بزن برام",
    "suck my cock": "کیرمو بخور", "deepthroat it": "تا ته حلقت ببرش پایین",
    "take it deep": "تا ته ببرش تو گلوت", "put it in your mouth": "بذارش تو دهنت",
    "open your mouth": "دهنتو باز کن", "stick out your tongue": "زبونتو دربیار",
    "lick it": "بلیسش", "lick my pussy": "کصمو بلیس", "eat my pussy": "کصمو بخور و بلیس",
    "eat me out": "برام بخورش", "lick my ass": "کونمو بلیس",
    "suck my tits": "ممه‌هامو بخور", "suck my nipples": "نوک ممه‌هامو بمک",
    "taste so good": "طعمش عالیه، خیلی خوشمزه‌ست", "you taste so good": "مزه تنت عالیه",
    "touch me": "به تنم دست بزن", "touch yourself": "خودتو بمال",
    "finger me": "انگشتم کن", "rub my clit": "چوچولمو بمال", "kiss me": "ببوس منو",
    "put it in": "بکنش تو", "put it inside": "بذارش توش", "slide it in": "آروم سرش بده تو",
    "shove it in": "محکم بکنش تو", "give it to me": "بده به من، بکنش تو",
    "take it all": "همه‌شو ببر تو", "ride me": "بیا روم بشین",
    "ride it": "بشین روش و بالا پایین کن", "ride my dick": "بیا بشین رو کیرم",
    "bounce on it": "بپر روش، بالا پایین کن", "get on top": "بیا بالا روم",
    "get on your knees": "بشین رو زانوهات", "on your knees": "برو رو زانوهات",
    "bend over": "خم شو", "turn around": "بچرخ اونوری",
    "spread your legs": "پاهاتو باز کن", "open your legs": "پاهاتو قشنگ باز کن",
    "show me your pussy": "کصتو نشونم بده", "show me your tits": "ممه‌هاتو نشونم بده",
    "show me your ass": "کونتو نشونم بده", "take it off": "دربیارش",
    "take your clothes off": "لباساتو دربیار", "strip for me": "برام لخت شو",
    "choke me": "گلومو بگیر", "spank me": "بزن رو کونم", "slap my ass": "محکم بزن در کونم",
    "pull my hair": "موهامو بکش", "grab my hips": "کمرمو سفت بگیر",
    "hold me tight": "محکم بغلم کن", "look at me": "نگام کن",
    "look into my eyes": "تو چشام نگاه کن", "look at you": "فدات بشم", "come here": "بیا اینجا",
}

SORTED_DIRECT_KEYS = sorted(
    [(k, v) for k, v in DIRECT_PHRASES.items() if len(k) >= 8],
    key=lambda x: len(x[0]),
    reverse=True
)

RAW_REPLACEMENTS = [
    (r"(?:من\s+)?دارم\s+می[\s\u200c]*آیم", "داره میاد"),
    (r"(?:من\s+)?دارم\s+میام", "داره میاد"),
    (r"(?:من\s+)?می[\s\u200c]*خوام\s+بیام", "داره آبم میاد"),
    (r"آبم\s+داره\s+میاد", "داره میاد"),
    (r"به\s+ارگاسم\s+می[\s\u200c]*رسم", "دارم ارضا میشم"),
    (r"ارگاسم", "ارضا شدن"),
    (r"منی\s+من", "آبم"),
    (r"منی\s+خود\s+را", "آبتو"),
    (r"اوه\s+بله", "آره جونم"),
    (r"اوه\s+آره", "آخ آره"),
    (r"بله\s+کودک", "آره عشقم"),
    (r"عزیزم\s+کودک", "عشقم"),
    (r"بله\s+عزیزم", "آره عشقم"),
    (r"بله\s+بابا", "آره ددی جون"),
    (r"اوه\s+خدای\s+من", "وای خدا"),
    (r"خدای\s+لعنتی\s+من", "وای خدای من"),
    (r"مرا\s+لعنت\s+کن(?:ید)?", "بکن منو"),
    (r"لعنت\s+به\s+من", "بکن منو"),
    (r"من\s+را\s+بکن", "بکن منو"),
    (r"مرا\s+بکن", "بکن منو"),
    (r"لعنتی\s+بله", "آخ آره همینه"),
    (r"لعنتی\s+خوب", "فوق‌العاده"),
    (r"لعنتی\s+داغ", "خیلی داغ و سکسی"),
    (r"لعنتی\s+عالی", "دیوونه‌کننده"),
    (r"فاک\s+می", "بکن منو"),
    (r"فاک", "لعنتی"),
    (r"سخت[\s\u200c]*تر", "محکم‌تر"),
    (r"سریع[\s\u200c]*تر", "تندتر"),
    (r"عمیق[\s\u200c]*تر", "تا ته"),
    (r"آهسته[\s\u200c]*تر", "آروم‌تر"),
    (r"تا\s+انتها", "تا تهش"),
    (r"متوقف\s+نشو(?:ید)?", "وای نستا"),
    (r"توقف\s+نکن(?:ید)?", "ول نکن"),
    (r"ایست\s+نکن", "ادامه بده"),
    (r"آن\s*را\s+بیرون\s+نکش(?:ید)?", "درش نیار"),
    (r"بیرون\s+نکش", "درش نیار"),
    (r"(?:آیا\s+)?(?:آن\s*را|اینو)\s+دوست\s+داری", "خوشت میاد؟"),
    (r"دوستش\s+داری\؟?", "حال می‌کنی؟"),
    (r"اینو\s+می[\s\u200c]*خواهی", "اینو می‌خوای؟"),
    (r"بسیار\s+خوب\s+است", "خیلی حال میده"),
    (r"احساس\s+خیلی\s+خوبی\s+دارد", "چقدر حال میده"),
    (r"حس\s+خیلی\s+خوبی\s+داره", "خیلی حال میده"),
    (r"خیلی\s+خوب\s+احساس\s+می[\s\u200c]*شه", "وای چه کیفی داره"),
    (r"مزه\s+بسیار\s+خوبی\s+دارد", "خیلی خوشمزه‌ست"),
    (r"طعم\s+خوبی\s+دارد", "مزه عالی میده"),
    (r"به\s+من\s+نگاه\s+کن(?:ید)?", "نگام کن"),
    (r"به\s+اینجا\s+بیا(?:یید)?", "بیا اینجا"),
    (r"بیا\s+این[\s\u200c]*جا", "بیا اینجا"),
    (r"آن\s*را\s+مکیدن", "بخورش"),
    (r"آن\s*را\s+بمک(?:ید)?", "بخورش"),
    (r"مک\s+بزن", "بخورش"),
    (r"آن\s*را\s+لیس\s+بزن(?:ید)?", "بلیسش"),
    (r"لیس\s+زدن", "لیسیدن"),
    (r"آن\s*را\s+قورت\s+بده", "قورتش بده"),
    (r"آن\s*را\s+ببلع(?:ید)?", "قورتش بده"),
    (r"سوار\s+من\s+شو", "بیا روم بشین"),
    (r"سوار\s+شو", "بشین روم"),
    (r"لباس[\s\u200c]*هایت\s+را\s+درآور", "لباساتو دربیار"),
    (r"آن\s*را\s+درآور(?:ید)?", "دربیارش"),
    (r"آن\s*را\s+قرار\s+دهید", "بکنش تو"),
    (r"بگذار\s+داخل", "بکنش تو"),
    (r"فشار\s+بده\s+داخل", "بکنش تو"),
    (r"فشار\s+بده", "بکنش تو"),
    (r"پاهای\s+خود\s+را\s+باز\s+کن(?:ید)?", "پاهاتو باز کن"),
    (r"روی\s+زانوهای\s+خود", "رو زانوهات"),
    (r"گربه\s*(?:من|ات|ت)?", "کصم"),
    (r"واژن\s*(?:من|ات|ت)?", "کصم"),
    (r"خروس\s*(?:من|ات|ت)?", "کیرم"),
    (r"آلت\s+تناسلی", "کیر"),
    (r"الاغ\s*(?:من|ات|ت)?", "کونم"),
    (r"باسن\s*(?:من|ات|ت)?", "کونم"),
    (r"سینه[\s\u200c]*هایم", "ممه‌هام"),
    (r"سینه[\s\u200c]*هایت", "ممه‌هات"),
    (r"سینه[\s\u200c]*ها", "ممه‌ها"),
    (r"نوک\s+پستان", "نوک ممه"),
]

COMPILED_REPLACEMENTS = [
    (re.compile(pattern, flags=re.IGNORECASE), repl)
    for pattern, repl in RAW_REPLACEMENTS
]


def clean_for_lookup(text: str) -> str:
    t = CLEAN_PUNCT_REGEX.sub("", text.lower().strip())
    return MULTI_SPACE_REGEX.sub(" ", t).strip()


def match_direct_phrase(text: str) -> str:
    cleaned = clean_for_lookup(text)
    if not cleaned:
        return ""
    if cleaned in DIRECT_PHRASES:
        return DIRECT_PHRASES[cleaned]

    clauses = [clean_for_lookup(c) for c in re.split(r"[,!.?;]+", text) if c.strip()]
    if len(clauses) > 1:
        matched_parts = []
        for c in clauses:
            if c in DIRECT_PHRASES:
                matched_parts.append(DIRECT_PHRASES[c])
            else:
                c_trimmed = MULTI_SPACE_REGEX.sub(" ", re.sub(r"\b(baby|babe|please|now|right now)\b", "", c)).strip()
                if c_trimmed in DIRECT_PHRASES:
                    matched_parts.append(DIRECT_PHRASES[c_trimmed])
        if len(matched_parts) == len(clauses):
            return "، ".join(matched_parts)

    for k, v in SORTED_DIRECT_KEYS:
        if k in cleaned:
            return v
    return ""


def refine_colloquial_persian(text: str) -> str:
    if not text:
        return ""
    t = text.strip()
    for compiled_re, repl in COMPILED_REPLACEMENTS:
        t = compiled_re.sub(repl, t)
    return MULTI_SPACE_REGEX.sub(" ", t).strip()


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


SYSTEM_TRANSLATION_PROMPT_UNCENSORED = (
    "You are an elite, uncensored Iranian cinema & adult romance subtitle translator.\n"
    "Translate every numbered English line into 100% natural, colloquial, passionate spoken Iranian Persian (فارسی کاملاً عامیانه، محاوره‌ای و بدون سانسور).\n"
    "CRITICAL RULES:\n"
    "1. Intimate & Erotic Slang: Translate erotic/intimate dialogue into authentic real-world spoken Persian "
    "(e.g. 'I\\'m coming' -> 'داره میاد / دارم ارضا میشم', 'Harder' -> 'محکم‌تر', 'Don\\'t stop' -> 'وای نستا / ول نکن', "
    "'Fuck me' -> 'بکن منو', 'Suck my dick' -> 'ساک بزن برام', 'Eat my pussy' -> 'کصمو بلیس', 'Ride me' -> 'بیا روم بشین').\n"
    "2. Zero Bookish Language: Never use literal/robotic translations (NO 'سخت‌تر', NO 'من دارم می‌آیم', NO 'کودک', NO 'خروس', NO 'گربه').\n"
    "3. Strict Output Format: Output ONLY: ID|||ترجمه روان فارسی"
)

SYSTEM_TRANSLATION_PROMPT_CINEMA = (
    "You are a master Iranian cinema subtitle translator.\n"
    "Translate every numbered English line into natural, fluent, colloquial Iranian Persian (فارسی محاوره‌ای و طبیعی سینمایی).\n"
    "Output ONLY: ID|||ترجمه روان فارسی"
)

GEMINI_SAFETY_OFF = [
    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
]


async def fast_gtx_single(client: httpx.AsyncClient, item: dict) -> tuple[int, str]:
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
    sem: asyncio.Semaphore | None = None,
    tone: str = "uncensored"
) -> dict[int, str]:
    input_lines = "\n".join(f"{item['id']}|||{item['text']}" for item in batch)
    sys_prompt = SYSTEM_TRANSLATION_PROMPT_UNCENSORED if tone != "cinema" else SYSTEM_TRANSLATION_PROMPT_CINEMA

    async def _run_with_client(client: httpx.AsyncClient) -> dict[int, str]:
        if GROQ_API_KEY:
            for groq_model in ("llama-3.1-8b-instant", "llama-3.3-70b-versatile"):
                try:
                    resp = await client.post(
                        "https://api.groq.com/openai/v1/chat/completions",
                        headers={"Authorization": f"Bearer {GROQ_API_KEY}"},
                        json={
                            "model": groq_model,
                            "messages": [
                                {"role": "system", "content": sys_prompt},
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

        if GEMINI_API_KEY:
            for gemini_model in ("gemini-2.5-flash", "gemini-2.0-flash"):
                try:
                    url = f"https://generativelanguage.googleapis.com/v1beta/models/{gemini_model}:generateContent?key={GEMINI_API_KEY}"
                    payload = {
                        "contents": [{"parts": [{"text": f"{sys_prompt}\n\nEnglish Subtitles:\n{input_lines}"}]}],
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
    if shared_client:
        return await _run_with_client(shared_client)
    async with httpx.AsyncClient(timeout=7.0) as temp_client:
        return await _run_with_client(temp_client)


async def translate_all_subtitles_instant(segments: list[dict], tone: str = "uncensored") -> list[dict]:
    if not segments:
        return []

    combined_map = {}
    remaining_segments = []
    for item in segments:
        direct = match_direct_phrase(item["text"]) if tone != "cinema" else ""
        if direct:
            combined_map[item["id"]] = direct
        else:
            remaining_segments.append(item)

    async with httpx.AsyncClient(timeout=7.0) as shared_client:
        if remaining_segments:
            batch_size = 15
            sem = asyncio.Semaphore(4)
            batches = [remaining_segments[i:i + batch_size] for i in range(0, len(remaining_segments), batch_size)]
            try:
                tasks = [try_fast_llm_batch(b, shared_client=shared_client, sem=sem, tone=tone) for b in batches]
                results = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=12.0)
                for r in results:
                    if isinstance(r, dict):
                        combined_map.update(r)
            except Exception:
                pass

        missing = [
            item for item in segments
            if item["id"] not in combined_map or not has_persian_chars(combined_map.get(item["id"], ""))
        ]
        if missing:
            try:
                joined = "\n".join(item["text"].replace("\n", " ").strip() for item in missing)
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

    translated_all = []
    for item in segments:
        cid = item["id"]
        fa_text = combined_map.get(cid, "").strip()
        fa_text = refine_colloquial_persian(fa_text) if fa_text else item["text"]
        translated_all.append({"start": item["start"], "end": item["end"], "text": fa_text})

    return translated_all


# ================= SUBTITLE (.ASS) & ULTRAFAST HIGH-QUALITY FFMPEG =================
ASS_COLOR_MAP = {
    "yellow": "&H0000FFFF",
    "white": "&H00FFFFFF",
    "cyan": "&H00FFFF00",
    "green": "&H0000FF00",
}

SIZE_MULTIPLIER_MAP = {"small": 0.84, "normal": 1.0, "large": 1.18}
TAG_ALIGNMENT_MAP = {"bottom": (2, 0.022), "top_right": (9, 0.035), "top_left": (7, 0.035)}


def format_ass_time(seconds: float) -> str:
    hrs = int(seconds // 3600)
    mins = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    centis = min(99, int(round((seconds - int(seconds)) * 100)))
    return f"{hrs}:{mins:02d}:{secs:02d}.{centis:02d}"


def generate_ass_file(
    subtitles: list[dict],
    user_tag: str,
    width: int,
    height: int,
    ass_path: str,
    options: dict | None = None
):
    opts = options or RENDER_OPTS
    color_code = ASS_COLOR_MAP.get(opts.get("color", "yellow"), "&H0000FFFF")
    size_mult = SIZE_MULTIPLIER_MAP.get(opts.get("size", "normal"), 1.0)
    tag_pos = opts.get("tag_pos", "bottom")

    min_dim = min(width, height)
    sub_font_size = max(22, int(min_dim * 0.046 * size_mult))
    tag_font_size = max(18, int(min_dim * 0.032))
    sub_margin_v = int(height * 0.085)
    outline_size = max(2, int(sub_font_size * 0.08))

    tag_align, tag_margin_ratio = TAG_ALIGNMENT_MAP.get(tag_pos, (2, 0.022))
    tag_margin_v = int(height * tag_margin_ratio)

    ass_header = f"""[Script Info]
ScriptType: v4.00+
PlayResX: {width}
PlayResY: {height}
WrapStyle: 0
ScaledBorderAndShadow: yes

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: YellowSub,Vazirmatn,{sub_font_size},{color_code},&H000000FF,&H00000000,&H80000000,-1,0,0,0,100,100,0,0,1,{outline_size},1,2,40,40,{sub_margin_v},1
Style: BottomTag,Vazirmatn,{tag_font_size},&H30FFFFFF,&H000000FF,&H00000000,&H60000000,-1,0,0,0,100,100,0,0,1,2,1,{tag_align},28,28,{tag_margin_v},1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""
    lines = [ass_header.strip()]

    if tag_pos != "none" and user_tag:
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
        target_max_kbps = max(2200, min(orig_bitrate_kbps, 6000))
        crf_val = "23"
    else:
        target_max_kbps = max(800, min(orig_bitrate_kbps, 3000))
        crf_val = "24"

    bufsize_kbps = target_max_kbps * 2

    def _build_ffmpeg_cmd(audio_codec_args: list[str]) -> list[str]:
        return [
            "ffmpeg", "-y", "-nostdin", "-threads", "0", "-thread_type", "slice+frame",
            "-i", input_video, "-vf", vf_filter, "-c:v", "libx264", "-preset", "ultrafast",
            "-tune", "fastdecode", "-crf", crf_val, "-maxrate", f"{target_max_kbps}k",
            "-bufsize", f"{bufsize_kbps}k", "-pix_fmt", "yuv420p", *audio_codec_args,
            "-movflags", "+faststart", "-progress", "pipe:1", "-nostats", output_video
        ]

    for attempt_idx, audio_args in enumerate((["-c:a", "copy"], ["-c:a", "aac", "-b:a", "128k"])):
        cmd = _build_ffmpeg_cmd(audio_args)
        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )

        while True:
            line = await process.stdout.readline()
            if not line:
                break
            line_str = line.decode("utf-8", errors="ignore").strip()
            if line_str.startswith("out_time_ms=") and total_duration > 0:
                try:
                    ms = int(line_str.split("=")[1])
                    pct = min(99.0, max(0.0, ((ms / 1_000_000.0) / total_duration) * 100.0))
                    bar = make_progress_bar(pct)
                    await update_status(
                        client,
                        f"🎬 <b>مرحله ۴ از ۵:</b> رندر توربو زیرنویس...\n{bar} <b>{pct:.1f}%</b>",
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
    for attempt in range(2):
        client = TelegramClient(session_path, API_ID, API_HASH, flood_sleep_threshold=60)
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
    t_job_start = time.time()
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
                await update_status(
                    main_client,
                    f"📥 <b>مرحله ۱ از ۵:</b> دانلود ۶ کاناله توربو...\n{bar} <b>{pct:.1f}%</b> (<code>{mb_cur:.1f} / {mb_tot:.1f} MB</code>)",
                    force=False
                )

        # 1. 6-Socket Striped Download
        t0_dl = time.time()
        await update_status(main_client, "📥 <b>مرحله ۱ از ۵:</b> شروع دانلود موازی ۶ کاناله...", force=True)
        video_msg = await main_client.get_messages(CHAT_ID, ids=VIDEO_MSG_ID)
        if not video_msg:
            raise RuntimeError("ویدیوی مورد نظر یافت نشد (ممکن است حذف شده باشد).")

        input_video = "input_video.mp4"
        await striped_download_file(main_client, video_msg, input_video, progress_callback=dl_progress, pool_size=6)
        dl_elapsed = time.time() - t0_dl

        thumb_path = None
        if THUMB_MSG_ID and THUMB_MSG_ID > 0:
            thumb_msg = await main_client.get_messages(CHAT_ID, ids=THUMB_MSG_ID)
            if thumb_msg and thumb_msg.photo:
                thumb_path = await main_client.download_media(thumb_msg, file="custom_thumb.jpg")

        orig_w, orig_h, duration, orig_fps, orig_bitrate_kbps = get_video_info(input_video)
        target_w, target_h = compute_target_resolution(orig_w, orig_h)
        print(
            f"[WORKER] Video info: {orig_w}x{orig_h} -> target: {target_w}x{target_h}, "
            f"duration: {duration}s, fps: {orig_fps:.2f}, bitrate: {orig_bitrate_kbps}k, opts: {RENDER_OPTS}",
            flush=True
        )

        # 2. Extract Fast 16kHz MP3 Audio & Transcribe with Groq Whisper Turbo
        t0_ai = time.time()
        await update_status(main_client, "🎙 <b>مرحله ۲ از ۵:</b> تشخیص سریع گفتار (Whisper Turbo)...", force=True)
        audio_path = "extracted_audio.mp3"
        extract_ultrafast_audio(input_video, audio_path)
        segments = transcribe_audio_fast(audio_path, duration=duration)

        if not segments:
            await update_status(main_client, "⚠️ گفتگوی انگلیسی تشخیص داده نشد، در حال درج تگ...", force=True)
            translated_subs = []
        else:
            # 3. Guaranteed Ultra-Fast Subtitle Translation
            await update_status(
                main_client,
                f"🧠 <b>مرحله ۳ از ۵:</b> ترجمه آنی هوشمند ({len(segments)} خط)...",
                force=True
            )
            translated_subs = await translate_all_subtitles_instant(
                segments, tone=RENDER_OPTS.get("tone", "uncensored")
            )
        ai_elapsed = time.time() - t0_ai

        # 4. Generate ASS & Burn with Ultrafast FFmpeg
        t0_burn = time.time()
        await update_status(main_client, "🎬 <b>مرحله ۴ از ۵:</b> شروع رندر توربو زیرنویس...", force=True)
        ass_path = "subtitles.ass"
        fonts_dir = "./fonts"
        output_video = "output_subtitled.mp4"

        generate_ass_file(translated_subs, USER_TAG, target_w, target_h, ass_path, options=RENDER_OPTS)
        await burn_subtitles_ffmpeg_size_capped(
            main_client, input_video, ass_path, fonts_dir, output_video,
            target_w, target_h, orig_w, orig_h, duration, orig_fps, orig_bitrate_kbps
        )
        burn_elapsed = time.time() - t0_burn

        if not thumb_path:
            thumb_path = "auto_thumb.jpg"
            subprocess.run(
                ["ffmpeg", "-y", "-nostdin", "-ss", "00:00:01", "-i", output_video, "-vframes", "1", "-q:v", "2", thumb_path],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            if not os.path.exists(thumb_path):
                thumb_path = None

        # 5. 6-Socket Striped Upload & Streaming Delivery
        t0_ul = time.time()
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
                await update_status(
                    main_client,
                    f"📤 <b>مرحله ۵ از ۵:</b> آپلود توربو ویدیوی نهایی...\n{bar} <b>{pct:.1f}%</b> (<code>{mb_cur:.1f} / {mb_tot:.1f} MB</code>)",
                    force=False
                )

        uploaded_input_file = await striped_upload_file(
            main_client, output_video, progress_callback=ul_progress, pool_size=6
        )
        ul_elapsed = time.time() - t0_ul

        safe_tag = USER_TAG.replace("<", "").replace(">", "")
        sent_video_msg = await main_client.send_file(
            CHAT_ID,
            file=uploaded_input_file,
            thumb=thumb_path,
            caption=f"✅ <b>ویدیوی زیرنویس‌شده آماده است!</b>\n🏷 <code>{safe_tag}</code>",
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

        # Send separate Completion Time & Performance Report replied directly to the translated video
        total_elapsed = time.time() - t_job_start
        completion_report = (
            "⏱ <b>گزارش زمان تکمیل و پردازش ابری (Turbo Pro)</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"⚡ <b>زمان کل تکمیل عملیات:</b> <code>{format_duration_fa(total_elapsed)}</code>\n"
            f"📥 <b>دانلود ۶ کاناله:</b> <code>{dl_elapsed:.1f} ثانیه</code>\n"
            f"🧠 <b>هوش مصنوعی (صوت و ترجمه):</b> <code>{ai_elapsed:.1f} ثانیه</code> (<code>{len(translated_subs)}</code> خط)\n"
            f"🎬 <b>رندر هاردساب FFmpeg:</b> <code>{burn_elapsed:.1f} ثانیه</code> (<code>{target_w}x{target_h}</code>)\n"
            f"📤 <b>آپلود ۶ کاناله:</b> <code>{ul_elapsed:.1f} ثانیه</code> (<code>{out_size_mb:.1f} MB</code>)\n"
            "━━━━━━━━━━━━━━━━━━━━"
        )
        await main_client.send_message(
            CHAT_ID,
            completion_report,
            reply_to=sent_video_msg.id,
            parse_mode="html"
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
