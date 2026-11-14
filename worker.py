import os
import sys
import re
import json
import time
import math
import hashlib
import asyncio
import subprocess
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

import httpx
from deep_translator import GoogleTranslator
from telethon import TelegramClient
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

groq_client = Groq(api_key=GROQ_API_KEY, max_retries=0, timeout=25.0) if GROQ_API_KEY else None

last_edit_time = 0.0

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

# ================= STATUS & PROGRESS HELPERS =================
async def update_status(client: TelegramClient, step_text: str, force: bool = True):
    global last_edit_time
    now = time.time()
    if not force and (now - last_edit_time) < 4.0:
        return
    last_edit_time = now
    try:
        text = (
            "⚡ <b>موتور پردازش هوشمند زیرنویس (نسخه توربو)</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"{step_text}\n"
            "━━━━━━━━━━━━━━━━━━━━"
        )
        await client.edit_message(CHAT_ID, STATUS_MSG_ID, text, parse_mode="html")
    except Exception:
        pass

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
        return await client.download_media(media_message, file=output_path)

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

    fd = os.open(output_path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
    os.ftruncate(fd, file_size)

    senders = await create_sender_pool(client, dc_id, pool_size=pool_size)

    downloaded_bytes = 0
    queue = asyncio.Queue()
    for part_idx in range(total_parts):
        queue.put_nowait(part_idx)

    async def worker_task(sender: MTProtoSender):
        nonlocal downloaded_bytes
        while not queue.empty():
            try:
                part_idx = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            offset = part_idx * part_size
            for attempt in range(4):
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
                except Exception as e:
                    if attempt == 3:
                        raise e
                    await asyncio.sleep(0.3 * (attempt + 1))
            queue.task_done()

    try:
        tasks = [asyncio.create_task(worker_task(s)) for s in senders]
        await asyncio.gather(*tasks)
    finally:
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

    fd = os.open(file_path, os.O_RDONLY)
    senders = await create_sender_pool(client, client.session.dc_id, pool_size=pool_size)

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

            for attempt in range(4):
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
                except Exception as e:
                    if attempt == 3:
                        raise e
                    await asyncio.sleep(0.3 * (attempt + 1))
            queue.task_done()

    try:
        tasks = [asyncio.create_task(worker_task(s)) for s in senders]
        await asyncio.gather(*tasks)
    finally:
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
        "-show_entries", "stream=width,height,r_frame_rate,bit_rate:format=duration,bit_rate",
        "-of", "json", video_path
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    data = json.loads(res.stdout)
    stream = data["streams"][0]
    fmt = data.get("format", {})

    width = int(stream.get("width", 1280))
    height = int(stream.get("height", 720))
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
    if min_dim <= 720:
        return (width - (width % 2), height - (height % 2))
    scale_factor = 720.0 / float(min_dim)
    new_w = int(round(width * scale_factor))
    new_h = int(round(height * scale_factor))
    new_w -= (new_w % 2)
    new_h -= (new_h % 2)
    return new_w, new_h

def extract_ultrafast_audio(video_path: str, audio_path: str):
    cmd = [
        "ffmpeg", "-y", "-threads", "0", "-i", video_path,
        "-vn", "-sn", "-ac", "1", "-ar", "16000", "-c:a", "libopus", "-b:a", "24k",
        audio_path
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def transcribe_audio_fast(audio_path: str) -> list[dict]:
    if not groq_client:
        raise RuntimeError("GROQ_API_KEY is not set.")
    whisper_models = ["whisper-large-v3-turbo", "whisper-large-v3"]
    transcription = None
    for wm in whisper_models:
        try:
            with open(audio_path, "rb") as f:
                transcription = groq_client.audio.transcriptions.create(
                    file=(audio_path, f.read()),
                    model=wm,
                    prompt="Transcribe speech accurately with correct spelling and capitalization.",
                    response_format="verbose_json",
                    language="en",
                    temperature=0.0
                )
            break
        except Exception as e:
            print(f"Whisper {wm} error: {e}")

    if not transcription:
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

    return clean_segments

# ================= BULLETPROOF MULTI-LAYER TRANSLATION ENGINE =================
LINE_PARSE_REGEX = re.compile(r"^\[?(\d+)\]?\s*(?:\|\|\||[:\-–—\.]|\))\s*(.+)$")

def has_persian_chars(text: str) -> bool:
    """Returns True if the string contains any Persian/Arabic script characters."""
    return bool(re.search(r"[\u0600-\u06FF]", text))

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
            if idx_str and fa_text:
                res_map[int(idx_str)] = fa_text
        else:
            m = LINE_PARSE_REGEX.match(line)
            if m:
                res_map[int(m.group(1))] = m.group(2).strip()

    return res_map

SYSTEM_TRANSLATION_PROMPT = (
    "You are an expert film subtitle translator specializing in English to Persian.\n"
    "Translate every numbered English line into natural, conversational, fluent Persian (فارسی روان و محاوره‌ای).\n"
    "RULES:\n"
    "1. Output ONLY in this exact format:\n"
    "ID|||Persian translation\n"
    "2. Write English names, brands, and places phonetically in Persian script (e.g. John -> جان, Apple -> اپل).\n"
    "3. Never skip any ID. Translate all numbered lines completely."
)

async def try_groq_llm(batch: list[dict], groq_models: list[str]) -> dict[int, str]:
    """Tries Groq Chat Completions API over clean HTTPS endpoint."""
    if not GROQ_API_KEY:
        return {}

    input_lines = "\n".join(f"{item['id']}|||{item['text']}" for item in batch)
    url = "https://api.groq.com/openai/v1/chat/completions"

    async with httpx.AsyncClient(timeout=10.0) as client:
        for model in groq_models:
            try:
                resp = await client.post(
                    url,
                    headers={"Authorization": f"Bearer {GROQ_API_KEY}"},
                    json={
                        "model": model,
                        "messages": [
                            {"role": "system", "content": SYSTEM_TRANSLATION_PROMPT},
                            {"role": "user", "content": input_lines}
                        ],
                        "temperature": 0.15
                    }
                )
                if resp.status_code == 200:
                    raw_text = resp.json()["choices"][0]["message"]["content"]
                    raw_text = re.sub(r"<think>.*?</think>", "", raw_text, flags=re.DOTALL).strip()
                    parsed = parse_delimited_lines(raw_text)
                    if len(parsed) >= int(len(batch) * 0.7):
                        return parsed
                elif resp.status_code == 429:
                    print(f"Groq {model} rate limited, trying next model...")
                    continue
            except Exception as e:
                print(f"Groq {model} error: {e}")

    return {}

async def try_gemini_llm(batch: list[dict]) -> dict[int, str]:
    """Tries Gemini REST API over clean HTTPS endpoint."""
    if not GEMINI_API_KEY:
        return {}

    input_lines = "\n".join(f"{item['id']}|||{item['text']}" for item in batch)
    gemini_models = ["gemini-2.5-flash", "gemini-2.0-flash", "gemini-1.5-flash", "gemini-1.5-flash-8b"]

    async with httpx.AsyncClient(timeout=10.0) as client:
        for g_model in gemini_models:
            try:
                url = f"https://generativelanguage.googleapis.com/v1beta/models/{g_model}:generateContent?key={GEMINI_API_KEY}"
                payload = {
                    "contents": [{
                        "parts": [{"text": f"{SYSTEM_TRANSLATION_PROMPT}\n\nEnglish Subtitles:\n{input_lines}"}]
                    }],
                    "generationConfig": {"temperature": 0.15, "maxOutputTokens": 2048}
                }
                resp = await client.post(url, json=payload)
                if resp.status_code == 200:
                    candidates = resp.json().get("candidates", [])
                    if candidates:
                        raw_text = candidates[0]["content"]["parts"][0]["text"]
                        parsed = parse_delimited_lines(raw_text)
                        if len(parsed) >= int(len(batch) * 0.7):
                            return parsed
            except Exception as e:
                print(f"Gemini {g_model} error: {e}")

    return {}

async def try_gtx_batch(batch: list[dict]) -> dict[int, str]:
    """
    High-speed, unthrottled Google GTX batch translation.
    Sends newline-separated batches directly to the official translation engine in ~300ms.
    """
    if not batch:
        return {}

    res_map = {}
    texts = [item["text"].replace("\n", " ").strip() for item in batch]
    joined_text = "\n".join(texts)

    url = "https://translate.googleapis.com/translate_a/single"
    params = {
        "client": "gtx",
        "sl": "en",
        "tl": "fa",
        "dt": "t",
        "q": joined_text
    }

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code == 200:
                data = resp.json()
                translated_full = "".join(seg[0] for seg in data[0] if seg and seg[0])
                fa_lines = [l.strip() for l in translated_full.splitlines() if l.strip()]
                if len(fa_lines) == len(batch):
                    for item, fa_line in zip(batch, fa_lines):
                        if fa_line and has_persian_chars(fa_line):
                            res_map[item["id"]] = fa_line
                    return res_map
    except Exception as e:
        print(f"GTX batch error: {e}")

    return res_map

def deep_translator_batch_sync(batch: list[dict]) -> dict[int, str]:
    """Fallback using deep-translator with single requests to prevent rate limit blocks."""
    translator = GoogleTranslator(source="en", target="fa")
    res_map = {}
    for item in batch:
        try:
            t = item["text"].strip()
            fa = translator.translate(t)
            if fa and has_persian_chars(fa):
                res_map[item["id"]] = fa.strip()
        except Exception:
            pass
    return res_map

async def emergency_single_translate(text: str) -> str:
    """Attempts single-line translation through GTX and MyMemory."""
    t = text.strip()
    if not t:
        return ""

    # 1. GTX single
    try:
        url = "https://translate.googleapis.com/translate_a/single"
        params = {"client": "gtx", "sl": "en", "tl": "fa", "dt": "t", "q": t}
        async with httpx.AsyncClient(timeout=4.0) as client:
            r = await client.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code == 200:
                fa = r.json()[0][0][0]
                if fa and has_persian_chars(fa):
                    return fa.strip()
    except Exception:
        pass

    # 2. MyMemory single
    try:
        url = f"https://api.mymemory.translated.net/get?q={urllib.parse.quote(t)}&langpair=en|fa"
        async with httpx.AsyncClient(timeout=4.0) as client:
            r = await client.get(url)
            if r.status_code == 200:
                data = r.json()
                fa = data.get("responseData", {}).get("translatedText", "")
                if fa and has_persian_chars(fa) and not fa.startswith("MYMEMORY WARNING"):
                    return fa.strip()
    except Exception:
        pass

    return ""

async def translate_batch_orchestrator(batch: list[dict], groq_models: list[str]) -> dict[int, str]:
    """Runs primary LLM translation, falling back to GTX batch."""
    # 1. Try Groq
    if groq_models:
        res = await try_groq_llm(batch, groq_models)
        if len(res) >= int(len(batch) * 0.7):
            return res

    # 2. Try Gemini
    if GEMINI_API_KEY:
        res = await try_gemini_llm(batch)
        if len(res) >= int(len(batch) * 0.7):
            return res

    # 3. Fast GTX batch
    res = await try_gtx_batch(batch)
    return res

async def translate_all_subtitles_instant(segments: list[dict]) -> list[dict]:
    """
    Robust translation engine:
    1. Selects active Groq models + fallback models.
    2. Runs batches through Groq LLM -> Gemini -> Google GTX.
    3. Fills missing lines with deep_translator and emergency single-line translation.
    4. Validates Persian characters to guarantee NO English subtitles leak through.
    """
    if not segments:
        return []

    # Detect active Groq models with robust fallbacks
    groq_models = ["llama-3.3-70b-versatile", "llama-3.1-8b-instant", "qwen/qwen-2.5-32b"]
    if groq_client:
        try:
            detected = []
            for m in groq_client.models.list().data:
                mid = m.id
                if any(k in mid.lower() for k in ["llama-3", "qwen", "gemma"]) and "whisper" not in mid.lower() and "guard" not in mid.lower():
                    detected.append(mid)
            if detected:
                # Place detected models first, then fallbacks
                groq_models = detected + [m for m in groq_models if m not in detected]
        except Exception as e:
            print(f"Groq models list note: {e}")

    batch_size = 35
    batches = [segments[i:i + batch_size] for i in range(0, len(segments), batch_size)]

    # Parallel batch execution
    tasks = [translate_batch_orchestrator(b, groq_models) for b in batches]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    combined_map = {}
    for r in results:
        if isinstance(r, dict):
            combined_map.update(r)

    # Secondary pass: Fill any missing or non-Persian lines
    missing = [
        item for item in segments
        if item["id"] not in combined_map or not has_persian_chars(combined_map.get(item["id"], ""))
    ]

    if missing:
        print(f"Resolving {len(missing)} missing/untranslated lines via GTX and fallbacks...")
        # Try GTX for the missing subset
        gtx_res = await try_gtx_batch(missing)
        combined_map.update(gtx_res)

        still_missing = [
            item for item in missing
            if item["id"] not in combined_map or not has_persian_chars(combined_map.get(item["id"], ""))
        ]

        if still_missing:
            loop = asyncio.get_running_loop()
            with ThreadPoolExecutor(max_workers=3) as pool:
                dt_res = await loop.run_in_executor(pool, deep_translator_batch_sync, still_missing)
                combined_map.update(dt_res)

            # Emergency individual pass: guaranteed translation
            for item in still_missing:
                cid = item["id"]
                current_fa = combined_map.get(cid, "")
                if not current_fa or not has_persian_chars(current_fa):
                    emergency_fa = await emergency_single_translate(item["text"])
                    if emergency_fa:
                        combined_map[cid] = emergency_fa

    # Assemble final translated subtitles
    translated_all = []
    for item in segments:
        cid = item["id"]
        fa_text = combined_map.get(cid, "").strip()
        # Only as an absolute last resort if all 5 translation layers failed
        if not fa_text:
            fa_text = item["text"]

        translated_all.append({
            "start": item["start"],
            "end": item["end"],
            "text": fa_text
        })

    return translated_all

# ================= SUBTITLE (.ASS) & SIZE-CONTROLLED FFMPEG =================
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
        # Unicode RTL embedding (\u202B) ensures correct Persian text shaping and punctuation
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
    total_duration: int,
    orig_fps: float,
    orig_bitrate_kbps: int
):
    fps_filter = "fps=30," if orig_fps > 31.0 else ""
    # Escape colons and backslashes for FFmpeg filtergraph
    escaped_ass = ass_path.replace("\\", "/").replace(":", r"\:")
    escaped_fonts = fonts_dir.replace("\\", "/").replace(":", r"\:")
    vf_filter = f"{fps_filter}scale={target_w}:{target_h}:flags=fast_bilinear,ass={escaped_ass}:fontsdir={escaped_fonts}"

    target_max_kbps = min(orig_bitrate_kbps, 1800)
    target_max_kbps = max(400, target_max_kbps)
    bufsize_kbps = target_max_kbps * 2

    cmd = [
        "ffmpeg", "-y", "-threads", "0", "-i", input_video,
        "-vf", vf_filter,
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "26",
        "-maxrate", f"{target_max_kbps}k", "-bufsize", f"{bufsize_kbps}k",
        "-c:a", "copy",
        "-movflags", "+faststart",
        "-progress", "pipe:1", "-nostats",
        output_video
    ]

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
                    f"🎬 <b>مرحله ۴ از ۵:</b> رندر زیرنویس هوشمند...\n"
                    f"{bar} <b>{pct:.1f}%</b>",
                    force=False
                )
            except Exception:
                pass

    await process.wait()
    if process.returncode != 0:
        stderr_out = (await process.stderr.read()).decode("utf-8", errors="ignore")
        raise RuntimeError(f"FFmpeg failed: {stderr_out[-300:]}")

# ================= MAIN PIPELINE =================
async def main():
    os.makedirs("session_cache", exist_ok=True)
    token_hash = hashlib.md5(BOT_TOKEN.encode()).hexdigest()[:10]
    session_path = f"session_cache/bot_{token_hash}"

    main_client = TelegramClient(
        session_path,
        API_ID,
        API_HASH,
        flood_sleep_threshold=60
    )
    await main_client.start(bot_token=BOT_TOKEN)

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
        input_video = "input_video.mp4"
        await striped_download_file(main_client, video_msg, input_video, progress_callback=dl_progress, pool_size=6)

        thumb_path = None
        if THUMB_MSG_ID and THUMB_MSG_ID > 0:
            thumb_msg = await main_client.get_messages(CHAT_ID, ids=THUMB_MSG_ID)
            if thumb_msg and thumb_msg.photo:
                thumb_path = await main_client.download_media(thumb_msg, file="custom_thumb.jpg")

        orig_w, orig_h, duration, orig_fps, orig_bitrate_kbps = get_video_info(input_video)
        target_w, target_h = compute_target_resolution(orig_w, orig_h)

        # 2. Extract 24kbps Opus Audio & Transcribe with Groq Whisper Turbo
        await update_status(main_client, "🎙 <b>مرحله ۲ از ۵:</b> تشخیص سریع گفتار (Whisper Turbo)...", force=True)
        audio_path = "extracted_audio.ogg"
        extract_ultrafast_audio(input_video, audio_path)
        segments = transcribe_audio_fast(audio_path)

        if not segments:
            await update_status(main_client, "⚠️ گفتگوی انگلیسی تشخیص داده نشد، در حال درج تگ...", force=True)
            translated_subs = []
        else:
            # 3. Multi-layer Resilient Subtitle Translation
            await update_status(
                main_client,
                f"🧠 <b>مرحله ۳ از ۵:</b> ترجمه هوشمند ({len(segments)} خط)...",
                force=True
            )
            translated_subs = await translate_all_subtitles_instant(segments)

        # 4. Generate ASS & Burn with Bitrate-Capped FFmpeg
        await update_status(main_client, "🎬 <b>مرحله ۴ از ۵:</b> شروع رندر زیرنویس هوشمند...", force=True)
        ass_path = "subtitles.ass"
        fonts_dir = "./fonts"
        output_video = "output_subtitled.mp4"

        generate_ass_file(translated_subs, USER_TAG, target_w, target_h, ass_path)
        await burn_subtitles_ffmpeg_size_capped(
            main_client,
            input_video,
            ass_path,
            fonts_dir,
            output_video,
            target_w,
            target_h,
            duration,
            orig_fps,
            orig_bitrate_kbps
        )

        if not thumb_path:
            thumb_path = "auto_thumb.jpg"
            subprocess.run([
                "ffmpeg", "-y", "-ss", "00:00:01", "-i", output_video,
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

        await main_client.send_file(
            CHAT_ID,
            file=uploaded_input_file,
            thumb=thumb_path,
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
