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

from telethon import TelegramClient, utils
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
from telethon.tl.functions.auth import (
    ExportAuthorizationRequest,
    ImportAuthorizationRequest,
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


# ================= STATUS & PROGRESS HELPERS =================
async def update_status(client: TelegramClient, step_text: str, force: bool = True):
    global last_edit_time
    now = time.time()
    if not force and (now - last_edit_time) < 5.5:
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


# ================= FLOOD-FREE 6-SOCKET MTPROTO SENDER ENGINE =================
async def create_sender_pool(client: TelegramClient, dc_id: int, pool_size: int = 6) -> list[MTProtoSender]:
    """
    Creates 6 raw MTProtoSender TCP connections WITHOUT calling sign_in(bot_token)!
    - If on the same DC as the main client, it reuses client.session.auth_key directly (0 auth calls!).
    - If on a different DC, it exports authorization once.
    This gives 6-socket parallel speed while making FloodWait on ImportBotAuthorization impossible!
    """
    dc = await client._get_dc(dc_id)
    senders = []

    if dc_id == client.session.dc_id:
        # Reuse the exact same auth_key across 6 independent TCP sockets (ZERO auth requests!)
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
        # Cross-DC file transfer: export once to get DC auth_key, then clone across 6 sockets
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
                    os.pwrite(fd, chunk, offset)
                    downloaded_bytes += len(chunk)
                    if progress_callback:
                        await progress_callback(downloaded_bytes, file_size)
                    break
                except Exception as e:
                    if attempt == 3:
                        raise e
                    await asyncio.sleep(0.4 * (attempt + 1))
            queue.task_done()

    try:
        tasks = []
        for s in senders:
            tasks.append(asyncio.create_task(worker_task(s)))
            tasks.append(asyncio.create_task(worker_task(s)))
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
            chunk = os.pread(fd, part_size, offset)

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
                    await asyncio.sleep(0.4 * (attempt + 1))
            queue.task_done()

    try:
        tasks = []
        for s in senders:
            tasks.append(asyncio.create_task(worker_task(s)))
            tasks.append(asyncio.create_task(worker_task(s)))
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
        "-vn", "-ac", "1", "-ar", "16000", "-c:a", "libopus", "-b:a", "24k",
        audio_path
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def transcribe_audio_fast(audio_path: str) -> list[dict]:
    whisper_models = ["whisper-large-v3-turbo", "whisper-large-v3"]
    transcription = None
    for wm in whisper_models:
        try:
            with open(audio_path, "rb") as f:
                transcription = groq_client.audio.transcriptions.create(
                    file=(audio_path, f.read()),
                    model=wm,
                    prompt="Transcribe with accurate spelling and capitalization for names, brands, and places.",
                    response_format="verbose_json",
                    language="en",
                    temperature=0.0
                )
            break
        except Exception as e:
            print(f"Whisper {wm} error: {e}")

    if not transcription:
        raise RuntimeError("Groq Whisper transcription failed.")

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


# ================= BULLETPROOF LINE-BY-LINE TRANSLATION ENGINE =================
def get_active_models_once() -> tuple[list[str], list[str]]:
    groq_models = []
    try:
        for m in groq_client.models.list().data:
            mid = m.id
            if not any(x in mid.lower() for x in ["whisper", "guard", "tts", "playai", "embed", "vision", "allam"]):
                groq_models.append(mid)
        preferred = ["llama-3.3", "qwen", "deepseek", "llama-3.1", "llama", "gemma"]
        groq_models.sort(key=lambda x: next((i for i, kw in enumerate(preferred) if kw in x.lower()), 99))
    except Exception as e:
        print(f"Groq list note: {e}")

    gemini_models = ["gemini-3.8-flash", "gemini-3.8-pro", "gemini-3.0-flash"]
    if gemini_client:
        try:
            for m in gemini_client.models.list():
                name = (getattr(m, "name", "") or "").replace("models/", "", 1)
                if "gemini" in name and "flash" in name and not any(x in name for x in ["tts", "audio", "image", "vision"]):
                    if name not in gemini_models:
                        gemini_models.append(name)
        except Exception:
            pass

    print(f"Active Groq Models: {groq_models[:4]} | Active Gemini Models: {gemini_models[:3]}")
    return groq_models[:4], gemini_models[:3]


LINE_PARSE_REGEX = re.compile(r"^\[?(\d+)\]?\s*(?:\|\|\||[:\-–—])\s*(.+)$")


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


def translate_chunk_delimited(
    batch: list[dict],
    batch_idx: int,
    groq_models: list[str],
    gemini_models: list[str]
) -> dict[int, str]:
    system_instruction = (
        "You are an elite Iranian subtitle translator. Translate every numbered English line into "
        "natural, fluent, conversational Persian (فارسی محاوره‌ای و طبیعی).\n"
        "STRICT RULES:\n"
        "1. Output ONLY in this exact format for every line:\n"
        "ID|||Persian translation\n"
        "Example:\n"
        "0|||سلام رفقا، خوش اومدین به این ویدیو\n"
        "1|||امروز می‌خوایم یه اتفاق عجیب رو بررسی کنیم\n"
        "2. 100% PERSIAN SCRIPT ONLY: Write all proper names, people, brands, and acronyms phonetically in Persian letters (حروف فارسی, e.g. 'MrBeast' -> 'میستر بیست', 'FBI' -> 'اف‌بی‌آی'). Never translate people's names into literal dictionary words.\n"
        "3. Translate EVERY single ID. Do not add any intro or markdown."
    )

    input_lines = "\n".join(f"{item['id']}|||{item['text']}" for item in batch)
    providers = ["groq", "gemini"] if (batch_idx % 2 == 0) else ["gemini", "groq"]

    for provider in providers:
        if provider == "groq" and groq_models:
            for q_model in groq_models:
                try:
                    resp = groq_client.chat.completions.create(
                        model=q_model,
                        messages=[
                            {"role": "system", "content": system_instruction},
                            {"role": "user", "content": input_lines}
                        ],
                        temperature=0.15
                    )
                    raw_text = resp.choices[0].message.content or ""
                    raw_text = re.sub(r"<think>.*?</think>", "", raw_text, flags=re.DOTALL).strip()
                    parsed = parse_delimited_lines(raw_text)
                    if len(parsed) >= max(1, int(len(batch) * 0.6)):
                        print(f"Batch {batch_idx} ({len(parsed)}/{len(batch)} lines) translated via Groq {q_model}")
                        return parsed
                except Exception as e:
                    print(f"Groq {q_model} batch {batch_idx} skip: {str(e)[:120]}")

        elif provider == "gemini" and gemini_client and gemini_models:
            for g_model in gemini_models:
                try:
                    resp = gemini_client.models.generate_content(
                        model=g_model,
                        contents=input_lines,
                        config=types.GenerateContentConfig(
                            system_instruction=system_instruction,
                            temperature=0.15
                        )
                    )
                    raw_text = resp.text or ""
                    parsed = parse_delimited_lines(raw_text)
                    if len(parsed) >= max(1, int(len(batch) * 0.6)):
                        print(f"Batch {batch_idx} ({len(parsed)}/{len(batch)} lines) translated via Gemini {g_model}")
                        return parsed
                except Exception as e:
                    print(f"Gemini {g_model} batch {batch_idx} skip: {str(e)[:120]}")

    return {}


async def translate_all_subtitles_instant(segments: list[dict]) -> list[dict]:
    if not segments:
        return []

    groq_models, gemini_models = get_active_models_once()

    batch_size = 40
    batches = [segments[i:i + batch_size] for i in range(0, len(segments), batch_size)]

    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=4) as pool:
        tasks = [
            loop.run_in_executor(
                pool, translate_chunk_delimited, batch, idx, groq_models, gemini_models
            )
            for idx, batch in enumerate(batches)
        ]
        results = await asyncio.gather(*tasks)

    combined_map = {}
    for r in results:
        combined_map.update(r)

    missing = [item for item in segments if item["id"] not in combined_map]
    if missing:
        print(f"Retrying {len(missing)} missed subtitle lines...")
        for i in range(0, len(missing), 30):
            chunk = missing[i:i + 30]
            filled = await loop.run_in_executor(
                None, translate_chunk_delimited, chunk, 99 + i, groq_models, gemini_models
            )
            combined_map.update(filled)

    translated_all = []
    for item in segments:
        fa_text = combined_map.get(item["id"], "").strip()
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
    vf_filter = f"{fps_filter}scale={target_w}:{target_h}:flags=fast_bilinear,ass={ass_path}:fontsdir={fonts_dir}"

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
                    f"🎬 <b>مرحله ۴ از ۵:</b> رندر زیرنویس با حفظ حجم اصلی...\n"
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
    # Using a persistent session file keyed by the first 8 chars of BOT_TOKEN
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

        # 1. True 6-Socket Striped Download (Uses MTProtoSenders, ZERO extra bot logins!)
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

        # 2. Extract Ultra-Light 24kbps Opus Audio & Transcribe with Groq Whisper
        await update_status(main_client, "🎙 <b>مرحله ۲ از ۵:</b> تشخیص سریع گفتار (Whisper Turbo)...", force=True)
        audio_path = "extracted_audio.ogg"
        extract_ultrafast_audio(input_video, audio_path)
        segments = transcribe_audio_fast(audio_path)

        if not segments:
            await update_status(main_client, "⚠️ گفتگوی انگلیسی تشخیص داده نشد، در حال درج تگ...", force=True)
            translated_subs = []
        else:
            # 3. Direct Line-by-Line Translation
            await update_status(
                main_client,
                f"🧠 <b>مرحله ۳ از ۵:</b> ترجمه آنی ({len(segments)} خط)...",
                force=True
            )
            translated_subs = await translate_all_subtitles_instant(segments)

        # 4. Generate ASS & Burn with Bitrate-Capped FFmpeg
        await update_status(main_client, "🎬 <b>مرحله ۴ از ۵:</b> شروع رندر زیرنویس با حجم بهینه...", force=True)
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
