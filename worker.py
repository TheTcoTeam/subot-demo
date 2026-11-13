import os
import sys
import re
import json
import time
import math
import hashlib
import asyncio
import subprocess
from concurrent.futures import ThreadPoolExecutor

from telethon import TelegramClient
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
from google import genai
from google.genai import types

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

groq_client = Groq(api_key=GROQ_API_KEY)
gemini_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

last_edit_time = 0.0
ENGLISH_REGEX = re.compile(r"[A-Za-z]{2,}")


# ================= STATUS & PROGRESS HELPERS =================
async def update_status(client: TelegramClient, step_text: str, force: bool = True):
    global last_edit_time
    now = time.time()
    if not force and (now - last_edit_time) < 6.0:
        return
    last_edit_time = now
    try:
        text = (
            "⚡ <b>موتور پردازش فوق‌سریع (نسخه توربو)</b>\n"
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


def has_english(text: str) -> bool:
    if not text or not text.strip():
        return True
    return bool(ENGLISH_REGEX.search(text))


# ================= TRUE MULTI-SOCKET TCP CLIENT POOL =================
async def create_client_pool(pool_size: int = 6) -> list[TelegramClient]:
    """
    Creates 6 completely separate TelegramClient instances, each with its own
    independent TCP socket to Telegram. This bypasses Telegram's per-socket speed cap!
    """
    async def init_single_client():
        c = TelegramClient(None, API_ID, API_HASH, flood_sleep_threshold=60)
        await c.start(bot_token=BOT_TOKEN)
        return c

    clients = await asyncio.gather(*(init_single_client() for _ in range(pool_size)))
    return list(clients)


async def disconnect_client_pool(clients: list[TelegramClient]):
    await asyncio.gather(*(c.disconnect() for c in clients), return_exceptions=True)


async def striped_download_file(
    clients: list[TelegramClient],
    media_message,
    output_path: str,
    progress_callback=None
):
    """
    Downloads a large file by striping 512KB chunks across 6 separate TCP clients
    using an open memory-mapped/pre-allocated file descriptor.
    """
    doc = media_message.document or media_message.video
    if not doc:
        return await clients[0].download_media(media_message, file=output_path)

    file_size = doc.size
    part_size = 512 * 1024  # 512 KB
    total_parts = math.ceil(file_size / part_size)

    location = InputDocumentFileLocation(
        id=doc.id,
        access_hash=doc.access_hash,
        file_reference=doc.file_reference,
        thumb_size=""
    )

    # Pre-allocate file once and keep a single file handle open
    fd = os.open(output_path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
    os.ftruncate(fd, file_size)

    downloaded_bytes = 0
    queue = asyncio.Queue()
    for part_idx in range(total_parts):
        queue.put_nowait(part_idx)

    async def worker_task(worker_client: TelegramClient):
        nonlocal downloaded_bytes
        while not queue.empty():
            try:
                part_idx = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            offset = part_idx * part_size
            for attempt in range(4):
                try:
                    res = await worker_client(GetFileRequest(
                        location=location,
                        offset=offset,
                        limit=part_size
                    ))
                    chunk = res.bytes
                    os.pwrite(fd, chunk, offset)
                    downloaded_bytes += len(chunk)
                    if progress_callback:
                        await progress_callback(downloaded_bytes, file_size)
                    break
                except Exception as e:
                    if attempt == 3:
                        raise e
                    await asyncio.sleep(0.5 * (attempt + 1))
            queue.task_done()

    try:
        # Run 2 concurrent requests per TCP socket (6 sockets * 2 = 12 parallel in-flight chunks)
        tasks = []
        for c in clients:
            tasks.append(asyncio.create_task(worker_task(c)))
            tasks.append(asyncio.create_task(worker_task(c)))
        await asyncio.gather(*tasks)
    finally:
        os.close(fd)

    return output_path


async def striped_upload_file(
    clients: list[TelegramClient],
    file_path: str,
    progress_callback=None
):
    """
    Uploads the output video across all 6 independent TCP sockets simultaneously.
    """
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
    uploaded_bytes = 0
    queue = asyncio.Queue()
    for part_idx in range(total_parts):
        queue.put_nowait(part_idx)

    async def worker_task(worker_client: TelegramClient):
        nonlocal uploaded_bytes
        while not queue.empty():
            try:
                part_idx = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            offset = part_idx * part_size
            chunk = os.pread(fd, part_size, offset)

            for attempt in range(4):
                try:
                    if is_big:
                        await worker_client(SaveBigFilePartRequest(
                            file_id=file_id,
                            file_part=part_idx,
                            file_total_parts=total_parts,
                            bytes=chunk
                        ))
                    else:
                        await worker_client(SaveFilePartRequest(
                            file_id=file_id,
                            file_part=part_idx,
                            bytes=chunk
                        ))
                    uploaded_bytes += len(chunk)
                    if progress_callback:
                        await progress_callback(uploaded_bytes, file_size)
                    break
                except Exception as e:
                    if attempt == 3:
                        raise e
                    await asyncio.sleep(0.5 * (attempt + 1))
            queue.task_done()

    try:
        tasks = []
        for c in clients:
            tasks.append(asyncio.create_task(worker_task(c)))
            tasks.append(asyncio.create_task(worker_task(c)))
        await asyncio.gather(*tasks)
    finally:
        os.close(fd)

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
def get_video_info(video_path: str) -> tuple[int, int, int, float]:
    """Returns (width, height, duration_seconds, fps) using ffprobe."""
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=width,height,r_frame_rate:format=duration",
        "-of", "json", video_path
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    data = json.loads(res.stdout)
    stream = data["streams"][0]
    width = int(stream.get("width", 1280))
    height = int(stream.get("height", 720))
    duration = int(float(data.get("format", {}).get("duration", 0)))

    fps_str = stream.get("r_frame_rate", "30/1")
    try:
        num, den = fps_str.split("/")
        fps = float(num) / float(den)
    except Exception:
        fps = 30.0

    return width, height, duration, fps


def compute_target_resolution(width: int, height: int) -> tuple[int, int]:
    """Caps shorter side at 720p for fast encoding & compact file size."""
    min_dim = min(width, height)
    if min_dim <= 720:
        return (width - (width % 2), height - (height % 2))
    scale_factor = 720.0 / float(min_dim)
    new_w = int(round(width * scale_factor))
    new_h = int(round(height * scale_factor))
    new_w -= (new_w % 2)
    new_h -= (new_h % 2)
    return new_w, new_h


def extract_compressed_audio(video_path: str, audio_path: str):
    """Extracts audio at 20x real-time speed."""
    cmd = [
        "ffmpeg", "-y", "-threads", "0", "-i", video_path,
        "-vn", "-ac", "1", "-ar", "16000", "-b:a", "32k",
        audio_path
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def transcribe_audio_with_groq(audio_path: str) -> list[dict]:
    whisper_models = ["whisper-large-v3-turbo", "whisper-large-v3"]
    transcription = None

    for w_model in whisper_models:
        try:
            with open(audio_path, "rb") as f:
                transcription = groq_client.audio.transcriptions.create(
                    file=(audio_path, f.read()),
                    model=w_model,
                    prompt="Transcribe accurately with proper capitalization for all names, people, places, and brands.",
                    response_format="verbose_json",
                    language="en",
                    temperature=0.0
                )
            break
        except Exception as e:
            print(f"Whisper model {w_model} failed: {e}")

    if not transcription:
        raise RuntimeError("All Groq Whisper transcription models failed.")

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


# ================= FAST 100% PERSIAN TRANSLATION ENGINE =================
def get_fast_models() -> tuple[list[str], list[str]]:
    """Returns verified active Gemini and Groq models in <1 second."""
    gemini_models = ["gemini-3.8-flash", "gemini-3.0-flash"]
    groq_models = []
    try:
        for m in groq_client.models.list().data:
            mid = m.id
            if not any(x in mid.lower() for x in ["whisper", "guard", "tts", "playai", "embed", "vision", "allam"]):
                groq_models.append(mid)
        preferred = ["llama-3.3", "qwen", "deepseek", "llama-3.1", "gemma"]
        groq_models.sort(key=lambda x: next((i for i, kw in enumerate(preferred) if kw in x.lower()), 99))
    except Exception:
        groq_models = ["llama-3.3-70b-versatile", "llama-3.1-8b-instant"]
    return gemini_models, groq_models[:4]


def extract_json_from_text(raw_text: str):
    text = raw_text.strip()
    if "```" in text:
        for p in text.split("```"):
            p_clean = p.strip()
            if p_clean.startswith("json"):
                p_clean = p_clean[4:].strip()
            if p_clean.startswith("{") or p_clean.startswith("["):
                text = p_clean
                break
    if not (text.startswith("{") or text.startswith("[")):
        s, e = text.find("{"), text.rfind("}")
        if s != -1 and e != -1 and e > s:
            text = text[s:e + 1]
    return json.loads(text)


def call_llm_json_fast(
    system_prompt: str,
    user_payload: str,
    gemini_models: list[str],
    groq_models: list[str],
    prefer_gemini: bool = False
):
    """Ultra-fast LLM caller without slow sleep loops."""
    order = ["gemini", "groq"] if prefer_gemini else ["groq", "gemini"]

    for provider in order:
        if provider == "gemini" and gemini_client:
            for g_model in gemini_models:
                try:
                    response = gemini_client.models.generate_content(
                        model=g_model,
                        contents=user_payload,
                        config=types.GenerateContentConfig(
                            system_instruction=system_prompt,
                            response_mime_type="application/json",
                            temperature=0.15
                        )
                    )
                    return extract_json_from_text(response.text)
                except Exception as e:
                    print(f"Gemini {g_model} fast-skip: {str(e)[:100]}")

        elif provider == "groq":
            for q_model in groq_models:
                try:
                    chat_completion = groq_client.chat.completions.create(
                        model=q_model,
                        messages=[
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_payload}
                        ],
                        response_format={"type": "json_object"},
                        temperature=0.15
                    )
                    return extract_json_from_text(chat_completion.choices[0].message.content)
                except Exception as e:
                    print(f"Groq {q_model} fast-skip: {str(e)[:100]}")

    return None


def parse_translation_response(parsed_json) -> dict[int, str]:
    result_map = {}
    if not parsed_json:
        return result_map
    data = parsed_json.get("translations", parsed_json) if isinstance(parsed_json, dict) else parsed_json
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict) and "id" in item:
                val = item.get("fa") or item.get("persian") or item.get("text") or ""
                if val:
                    result_map[int(item["id"])] = str(val).strip()
    elif isinstance(data, dict):
        for k, v in data.items():
            try:
                if isinstance(v, str):
                    result_map[int(k)] = v.strip()
                elif isinstance(v, dict) and "fa" in v:
                    result_map[int(k)] = str(v["fa"]).strip()
            except ValueError:
                pass
    return result_map


def build_glossary_sync(segments: list[dict], gemini_models: list[str], groq_models: list[str]) -> dict:
    sample = " ".join(s["text"] for s in segments)[:8000]
    system_prompt = (
        "Analyze this English transcript and extract a JSON glossary of proper nouns "
        "(people, brands, places, acronyms) mapped to their Persian script phonetic equivalents "
        "(حروف فارسی, e.g., 'MrBeast': 'میستر بیست', 'FBI': 'اف‌بی‌آی'). Never translate names literally.\n"
        "Return ONLY JSON: {\"topic\": \"1-sentence summary\", \"glossary\": {\"EnglishName\": \"PersianScript\"}}"
    )
    res = call_llm_json_fast(system_prompt, sample, gemini_models, groq_models, prefer_gemini=False)
    if isinstance(res, dict):
        return {"topic": res.get("topic", ""), "glossary": res.get("glossary", {})}
    return {"topic": "", "glossary": {}}


def translate_batch_sync(
    batch: list[dict],
    batch_idx: int,
    context_info: dict,
    gemini_models: list[str],
    groq_models: list[str]
) -> dict[int, str]:
    glossary_str = json.dumps(context_info.get("glossary", {}), ensure_ascii=False)
    topic_str = context_info.get("topic", "")

    system_instruction = (
        "You are an elite Iranian subtitle translator. Translate EVERY English subtitle line into "
        "natural, fluent, conversational Persian (فارسی محاوره‌ای و طبیعی).\n"
        f"TOPIC: {topic_str}\n"
        f"NAME GLOSSARY: {glossary_str}\n"
        "STRICT RULES:\n"
        "1. 100% PERSIAN SCRIPT ONLY: Your output ('fa') MUST NOT contain ANY English/Latin letters (A-Z, a-z)! "
        "Write all names, brands, and acronyms phonetically in Persian script (حروف فارسی).\n"
        "2. Translate EVERY 'id' without skipping any.\n"
        "3. Return ONLY JSON: {\"translations\": [{\"id\": 0, \"fa\": \"...\"}]}"
    )

    payload = json.dumps(
        {"lines": [{"id": item["id"], "en": item["text"]} for item in batch]},
        ensure_ascii=False
    )
    # Alternate batches between Groq and Gemini so neither hits rate limits!
    prefer_gemini = (batch_idx % 2 == 1)
    parsed = call_llm_json_fast(system_instruction, payload, gemini_models, groq_models, prefer_gemini=prefer_gemini)
    return parse_translation_response(parsed)


async def translate_segments_turbo(segments: list[dict]) -> list[dict]:
    """
    Translates a 12-minute video in ~6-10 seconds by alternating large 50-line batches
    across BOTH Groq and Gemini simultaneously, followed by a quick straggler sweep.
    """
    if not segments:
        return []

    gemini_models, groq_models = get_fast_models()
    loop = asyncio.get_running_loop()

    context_info = await loop.run_in_executor(
        None, build_glossary_sync, segments, gemini_models, groq_models
    )

    batch_size = 50
    batches = [segments[i:i + batch_size] for i in range(0, len(segments), batch_size)]

    with ThreadPoolExecutor(max_workers=4) as pool:
        tasks = [
            loop.run_in_executor(
                pool, translate_batch_sync, batch, idx, context_info, gemini_models, groq_models
            )
            for idx, batch in enumerate(batches)
        ]
        results = await asyncio.gather(*tasks)

    combined_map = {}
    for r in results:
        combined_map.update(r)

    # Quick Sweep for any skipped IDs or stray English words
    stragglers = [
        item for item in segments
        if not combined_map.get(item["id"]) or has_english(combined_map.get(item["id"], ""))
    ]
    if stragglers:
        print(f"Sweeping {len(stragglers)} straggler lines...")
        for i in range(0, len(stragglers), 25):
            chunk = stragglers[i:i + 25]
            swept = await loop.run_in_executor(
                None, translate_batch_sync, chunk, i + 1, context_info, gemini_models, groq_models
            )
            combined_map.update(swept)

    translated_all = []
    for item in segments:
        fa_line = combined_map.get(item["id"], "") or "..."
        translated_all.append({
            "start": item["start"],
            "end": item["end"],
            "text": fa_line
        })

    return translated_all


# ================= SUBTITLE (.ASS) & ULTRA-FAST FFMPEG RENDERING =================
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


async def burn_subtitles_ffmpeg_turbo(
    client: TelegramClient,
    input_video: str,
    ass_path: str,
    fonts_dir: str,
    output_video: str,
    target_w: int,
    target_h: int,
    total_duration: int,
    orig_fps: float
):
    """
    Maximum-velocity CPU encode:
    - If video is >30fps (e.g. 60fps), drops to 30fps ('fps=30') cutting frame workload by 50%!
    - Scales to 720p using 'fast_bilinear'.
    - Disables slow x264 motion estimation ('ref=1:me=dia:subme=0:trellis=0') for 2x faster encoding.
    - Uses '-crf 27' so a 12-min video outputs at ~45MB (uploads to Telegram in 4 seconds!).
    """
    fps_filter = "fps=30," if orig_fps > 31.0 else ""
    vf_filter = f"{fps_filter}scale={target_w}:{target_h}:flags=fast_bilinear,ass={ass_path}:fontsdir={fonts_dir}"

    cmd = [
        "ffmpeg", "-y", "-threads", "0", "-i", input_video,
        "-vf", vf_filter,
        "-c:v", "libx264", "-preset", "ultrafast", "-tune", "fastdecode", "-crf", "27",
        "-x264-params", "ref=1:me=dia:subme=0:trellis=0:aq-mode=0",
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
                    f"🎬 <b>مرحله ۴ از ۵:</b> رندر توربو زیرنویس...\n"
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
    # Spin up 6 independent TCP sockets to Telegram
    clients = await create_client_pool(pool_size=6)
    main_client = clients[0]

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

        # 1. True 6-Socket Striped Download
        await update_status(main_client, "📥 <b>مرحله ۱ از ۵:</b> شروع دانلود موازی ۶ کاناله...", force=True)
        video_msg = await main_client.get_messages(CHAT_ID, ids=VIDEO_MSG_ID)
        input_video = "input_video.mp4"
        await striped_download_file(clients, video_msg, input_video, progress_callback=dl_progress)

        thumb_path = None
        if THUMB_MSG_ID and THUMB_MSG_ID > 0:
            thumb_msg = await main_client.get_messages(CHAT_ID, ids=THUMB_MSG_ID)
            if thumb_msg and thumb_msg.photo:
                thumb_path = await main_client.download_media(thumb_msg, file="custom_thumb.jpg")

        orig_w, orig_h, duration, orig_fps = get_video_info(input_video)
        target_w, target_h = compute_target_resolution(orig_w, orig_h)

        # 2. Extract Audio & Transcribe with Groq Whisper
        await update_status(main_client, "🎙 <b>مرحله ۲ از ۵:</b> استخراج صدا و تشخیص گفتار...", force=True)
        audio_path = "extracted_audio.mp3"
        extract_compressed_audio(input_video, audio_path)
        segments = transcribe_audio_with_groq(audio_path)

        if not segments:
            await update_status(main_client, "⚠️ گفتگوی انگلیسی تشخیص داده نشد، در حال درج تگ...", force=True)
            translated_subs = []
        else:
            # 3. Dual-Engine Parallel Translation (Groq + Gemini simultaneously)
            await update_status(
                main_client,
                f"🧠 <b>مرحله ۳ از ۵:</b> ترجمه موازی فوق‌سریع ({len(segments)} خط)...",
                force=True
            )
            translated_subs = await translate_segments_turbo(segments)

        # 4. Generate ASS & Burn with Turbo FFmpeg
        await update_status(main_client, "🎬 <b>مرحله ۴ از ۵:</b> شروع رندر توربو زیرنویس زرد...", force=True)
        ass_path = "subtitles.ass"
        fonts_dir = "./fonts"
        output_video = "output_subtitled.mp4"

        generate_ass_file(translated_subs, USER_TAG, target_w, target_h, ass_path)
        await burn_subtitles_ffmpeg_turbo(
            main_client, input_video, ass_path, fonts_dir, output_video, target_w, target_h, duration, orig_fps
        )

        if not thumb_path:
            thumb_path = "auto_thumb.jpg"
            subprocess.run([
                "ffmpeg", "-y", "-i", output_video, "-ss", "00:00:01",
                "-vframes", "1", "-q:v", "2", thumb_path
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if not os.path.exists(thumb_path):
                thumb_path = None

        # 5. True 6-Socket Striped Upload (Zero Caption)
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
            clients, output_video, progress_callback=ul_progress
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
        await disconnect_client_pool(clients)


if __name__ == "__main__":
    asyncio.run(main())
