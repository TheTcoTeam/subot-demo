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
    if not force and (now - last_edit_time) < 6.5:
        return
    last_edit_time = now
    try:
        text = (
            "⚡ <b>پردازشگر فوق‌سریع ابری فعال است...</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"{step_text}\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "⏳ <i>عملیات به صورت موازی در حال انجام است.</i>"
        )
        await client.edit_message(CHAT_ID, STATUS_MSG_ID, text, parse_mode="html")
    except Exception as e:
        print(f"Status update skipped: {e}")


def make_progress_bar(percent: float) -> str:
    filled = int(round(percent / 10))
    filled = max(0, min(10, filled))
    return "🟩" * filled + "⬜" * (10 - filled)


def has_english(text: str) -> bool:
    """Returns True if the subtitle line still contains English words/letters."""
    if not text or not text.strip():
        return True
    return bool(ENGLISH_REGEX.search(text))


# ================= 8-SOCKET PARALLEL MTPROTO TRANSFER ENGINE =================
async def parallel_download_file(
    client: TelegramClient,
    media_message,
    output_path: str,
    progress_callback=None,
    workers: int = 8
):
    doc = media_message.document or media_message.video
    if not doc:
        return await client.download_media(media_message, file=output_path)

    file_size = doc.size
    part_size = 512 * 1024  # 512 KB
    total_parts = math.ceil(file_size / part_size)

    location = InputDocumentFileLocation(
        id=doc.id,
        access_hash=doc.access_hash,
        file_reference=doc.file_reference,
        thumb_size=""
    )

    with open(output_path, "wb") as f:
        f.truncate(file_size)

    downloaded_bytes = 0
    sem = asyncio.Semaphore(workers)
    file_lock = asyncio.Lock()

    async def fetch_part(part_idx: int):
        nonlocal downloaded_bytes
        offset = part_idx * part_size
        async with sem:
            for attempt in range(4):
                try:
                    res = await client(GetFileRequest(
                        location=location,
                        offset=offset,
                        limit=part_size
                    ))
                    chunk = res.bytes
                    async with file_lock:
                        with open(output_path, "r+b") as f:
                            f.seek(offset)
                            f.write(chunk)
                        downloaded_bytes += len(chunk)
                        if progress_callback:
                            await progress_callback(downloaded_bytes, file_size)
                    break
                except Exception as e:
                    if attempt == 3:
                        raise e
                    await asyncio.sleep(1.0 * (attempt + 1))

    tasks = [asyncio.create_task(fetch_part(i)) for i in range(total_parts)]
    await asyncio.gather(*tasks)
    return output_path


async def parallel_upload_file(
    client: TelegramClient,
    file_path: str,
    progress_callback=None,
    workers: int = 8
):
    file_size = os.path.getsize(file_path)
    part_size = 512 * 1024  # 512 KB
    total_parts = math.ceil(file_size / part_size)
    is_big = file_size > 10 * 1024 * 1024
    file_id = int.from_bytes(os.urandom(8), signed=True)

    uploaded_bytes = 0
    sem = asyncio.Semaphore(workers)
    hash_md5 = hashlib.md5() if not is_big else None

    if not is_big:
        with open(file_path, "rb") as f:
            while chunk := f.read(65536):
                hash_md5.update(chunk)

    async def send_part(part_idx: int):
        nonlocal uploaded_bytes
        offset = part_idx * part_size
        with open(file_path, "rb") as f:
            f.seek(offset)
            chunk = f.read(part_size)

        async with sem:
            for attempt in range(4):
                try:
                    if is_big:
                        await client(SaveBigFilePartRequest(
                            file_id=file_id,
                            file_part=part_idx,
                            file_total_parts=total_parts,
                            bytes=chunk
                        ))
                    else:
                        await client(SaveFilePartRequest(
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
                    await asyncio.sleep(1.0 * (attempt + 1))

    tasks = [asyncio.create_task(send_part(i)) for i in range(total_parts)]
    await asyncio.gather(*tasks)

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
def get_video_info(video_path: str) -> tuple[int, int, int]:
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=width,height:format=duration",
        "-of", "json", video_path
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    data = json.loads(res.stdout)
    width = int(data["streams"][0].get("width", 1280))
    height = int(data["streams"][0].get("height", 720))
    duration = int(float(data.get("format", {}).get("duration", 0)))
    return width, height, duration


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


def extract_compressed_audio(video_path: str, audio_path: str):
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


# ================= 100% PERSIAN TRANSLATION ENGINE (ZERO ENGLISH) =================
def get_available_gemini_models() -> list[str]:
    candidates = ["gemini-3.8-flash", "gemini-3.8-pro", "gemini-3.0-flash", "gemini-2.5-pro"]
    if not gemini_client:
        return []
    try:
        for m in gemini_client.models.list():
            name = getattr(m, "name", "") or ""
            if name.startswith("models/"):
                name = name.replace("models/", "", 1)
            if "gemini" in name and not any(x in name for x in ["embedding", "imagen", "veo", "audio", "tts", "vision", "aqa"]):
                if name not in candidates:
                    candidates.append(name)
    except Exception as e:
        print(f"Could not list Gemini models dynamically: {e}")
    return candidates[:5]


def get_available_groq_chat_models() -> list[str]:
    candidates = []
    try:
        models_resp = groq_client.models.list()
        for m in models_resp.data:
            mid = m.id
            if not any(x in mid.lower() for x in ["whisper", "guard", "tts", "playai", "embed", "vision", "allam"]):
                candidates.append(mid)
    except Exception as e:
        print(f"Could not list Groq models dynamically: {e}")

    preferred_keywords = ["llama-3.3", "qwen", "deepseek", "llama-3.1", "llama", "gemma", "mixtral"]
    candidates.sort(
        key=lambda x: next((i for i, kw in enumerate(preferred_keywords) if kw in x.lower()), 99)
    )
    return candidates


def extract_json_from_text(raw_text: str):
    text = raw_text.strip()
    if "```" in text:
        parts = text.split("```")
        for p in parts:
            p_clean = p.strip()
            if p_clean.startswith("json"):
                p_clean = p_clean[4:].strip()
            if p_clean.startswith("{") or p_clean.startswith("["):
                text = p_clean
                break
    # Fallback: find first '{' and last '}'
    if not (text.startswith("{") or text.startswith("[")):
        start_brace = text.find("{")
        end_brace = text.rfind("}")
        if start_brace != -1 and end_brace != -1 and end_brace > start_brace:
            text = text[start_brace:end_brace + 1]
    return json.loads(text)


def call_llm_json(system_prompt: str, user_payload: str, gemini_models: list[str], groq_models: list[str]):
    """
    Tries Gemini and Groq with automatic rate-limit backoff and strict JSON mode
    so batches never fail or drop lines.
    """
    # 1. Try Groq models (with JSON mode first, then standard mode)
    for q_model in groq_models[:4]:
        for attempt in range(2):
            try:
                kwargs = {
                    "model": q_model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_payload}
                    ],
                    "temperature": 0.15
                }
                if attempt == 0:
                    kwargs["response_format"] = {"type": "json_object"}
                chat_completion = groq_client.chat.completions.create(**kwargs)
                return extract_json_from_text(chat_completion.choices[0].message.content)
            except Exception as e:
                err_str = str(e)
                if "429" in err_str or "rate_limit" in err_str.lower():
                    time.sleep(2.5 * (attempt + 1))
                else:
                    print(f"Groq {q_model} attempt {attempt+1} note: {err_str[:120]}")

    # 2. Try Gemini models
    if gemini_client:
        for g_model in gemini_models:
            for attempt in range(2):
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
                    err_str = str(e)
                    print(f"Gemini {g_model} attempt {attempt+1} note: {err_str[:120]}")
                    if "503" in err_str or "429" in err_str:
                        time.sleep(2.5)
                    else:
                        break

    return None


def build_video_context_and_name_glossary(
    segments: list[dict],
    gemini_models: list[str],
    groq_models: list[str]
) -> dict:
    """PASS 1: Builds a global Persian glossary of all proper names/brands/terms."""
    full_script_sample = " ".join(s["text"] for s in segments)
    if len(full_script_sample) > 12000:
        third = len(full_script_sample) // 3
        full_script_sample = (
            full_script_sample[:4000] + " ... " +
            full_script_sample[third:third + 4000] + " ... " +
            full_script_sample[-4000:]
        )

    glossary_system = (
        "You are a senior Persian localization director. Analyze the English video transcript below.\n"
        "Your job is to fix proper nouns (names of people, characters, celebrities, brands, games, places, acronyms) "
        "so the Persian subtitle translators write EVERYTHING in Persian script (حروف فارسی).\n"
        "Rules:\n"
        "1. Identify all proper nouns and acronyms in the transcript.\n"
        "2. Map each name/term to its natural Persian phonetic script (e.g., 'MrBeast' -> 'میستر بیست', 'FBI' -> 'اف‌بی‌آی', 'YouTube' -> 'یوتیوب').\n"
        "3. NEVER translate a person's name into its literal dictionary meaning.\n"
        "4. Return ONLY a valid JSON object with two keys:\n"
        "   - \"topic\": A 1-sentence summary of what the video is about.\n"
        "   - \"glossary\": An object mapping each English name/term to its exact Persian script equivalent."
    )

    res = call_llm_json(glossary_system, full_script_sample, gemini_models, groq_models)
    if isinstance(res, dict):
        return {
            "topic": res.get("topic", "General English video"),
            "glossary": res.get("glossary", {})
        }
    return {"topic": "General English video", "glossary": {}}


def parse_translation_response(parsed_json) -> dict[int, str]:
    """Handles both list of objects [{'id': 0, 'fa': '...'}] and dict mappings {'0': '...'}."""
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


def translate_single_batch_sync(
    batch: list[dict],
    prev_context: list[str],
    next_context: list[str],
    batch_idx: int,
    context_info: dict,
    gemini_models: list[str],
    groq_models: list[str]
) -> dict[int, str]:
    """PASS 2: Translates a 20-line batch with strict 100% Persian script enforcement."""
    glossary_str = json.dumps(context_info.get("glossary", {}), ensure_ascii=False)
    topic_str = context_info.get("topic", "")

    system_instruction = (
        "You are an elite Iranian subtitle translator. Translate EVERY English subtitle line into "
        "natural, fluent, conversational Persian (فارسی محاوره‌ای و طبیعی).\n\n"
        f"VIDEO TOPIC & TONE: {topic_str}\n"
        f"MANDATORY NAME & TERM GLOSSARY: {glossary_str}\n\n"
        "CRITICAL RULES (ZERO ENGLISH ALLOWED):\n"
        "1. 100% PERSIAN SCRIPT ONLY: Your output ('fa') MUST NOT contain ANY English/Latin letters (A-Z, a-z) whatsoever! "
        "Every single word, including proper names, brand names, places, and acronyms (like NASA -> ناسا, GTA -> جی‌تی‌ای, iPhone -> آیفون) MUST be written in Persian script (حروف فارسی).\n"
        "2. TRANSLATE EVERY ID: You MUST return a translation for every single 'id' in 'lines_to_translate'. Never skip any ID.\n"
        "3. PROPER NAMES: Transliterate people's names phonetically into Persian script (never translate names into literal dictionary meanings).\n"
        "4. FLOW: Read the previous/upcoming context lines so split sentences connect naturally.\n"
        "5. Return ONLY a JSON object with a single key \"translations\" containing the array of objects: "
        "[{\"id\": 0, \"fa\": \"...\"}, ...]"
    )

    payload_obj = {
        "previous_context_do_not_translate": prev_context,
        "lines_to_translate": [{"id": item["id"], "en": item["text"]} for item in batch],
        "upcoming_context_do_not_translate": next_context
    }
    prompt_payload = json.dumps(payload_obj, ensure_ascii=False)

    parsed_json = call_llm_json(system_instruction, prompt_payload, gemini_models, groq_models)
    return parse_translation_response(parsed_json)


def force_translate_stragglers(
    straggler_items: list[dict],
    context_info: dict,
    gemini_models: list[str],
    groq_models: list[str]
) -> dict[int, str]:
    """
    PASS 3 (THE SWEEP): Any line that was skipped by a batch OR still contains
    English characters [A-Za-z] is sent here in small chunks of 10 to force 100% Persian output.
    """
    fixed_map = {}
    glossary_str = json.dumps(context_info.get("glossary", {}), ensure_ascii=False)

    sweep_system = (
        "You are a Persian subtitle quality-control translator. "
        "The following subtitle lines either failed translation or still contain English letters.\n"
        f"GLOSSARY: {glossary_str}\n"
        "RULES:\n"
        "1. Translate every line completely into natural conversational Persian (فارسی محاوره‌ای).\n"
        "2. ZERO LATIN CHARACTERS ALLOWED: Do not use any English letters (a-z, A-Z) at all. Write all names, brands, and acronyms phonetically in Persian alphabet (حروف فارسی).\n"
        "3. Return ONLY a JSON object: {\"translations\": [{\"id\": <number>, \"fa\": \"<100% Persian text>\"}]}"
    )

    chunk_size = 12
    for i in range(0, len(straggler_items), chunk_size):
        chunk = straggler_items[i:i + chunk_size]
        payload = json.dumps(
            {"lines_to_translate": [{"id": x["id"], "en": x["text"]} for x in chunk]},
            ensure_ascii=False
        )
        parsed = call_llm_json(sweep_system, payload, gemini_models, groq_models)
        chunk_map = parse_translation_response(parsed)
        fixed_map.update(chunk_map)

    return fixed_map


async def translate_segments_parallel(segments: list[dict]) -> list[dict]:
    """
    Runs the complete 3-Pass Translation Pipeline:
    - Pass 1: Global Name & Brand Glossary
    - Pass 2: Parallel Contextual Translation (20 lines per batch, 2 concurrent workers to avoid 429 rate limits)
    - Pass 3: Automatic Straggler & English-Letter Sweep (Guarantees 0% English left behind)
    """
    if not segments:
        return []

    gemini_models = get_available_gemini_models()
    groq_models = get_available_groq_chat_models()

    loop = asyncio.get_running_loop()

    # Pass 1: Global Glossary
    context_info = await loop.run_in_executor(
        None,
        build_video_context_and_name_glossary,
        segments,
        gemini_models,
        groq_models
    )

    # Pass 2: Smaller 20-line batches with 2 workers so Groq/Gemini never rate-limit or skip IDs
    batch_size = 20
    tasks = []

    with ThreadPoolExecutor(max_workers=2) as pool:
        for idx, start_i in enumerate(range(0, len(segments), batch_size)):
            end_i = start_i + batch_size
            batch = segments[start_i:end_i]
            prev_ctx = [s["text"] for s in segments[max(0, start_i - 2):start_i]]
            next_ctx = [s["text"] for s in segments[end_i:min(len(segments), end_i + 2)]]

            tasks.append(
                loop.run_in_executor(
                    pool,
                    translate_single_batch_sync,
                    batch,
                    prev_ctx,
                    next_ctx,
                    idx,
                    context_info,
                    gemini_models,
                    groq_models
                )
            )
        results = await asyncio.gather(*tasks)

    combined_map = {}
    for res_map in results:
        combined_map.update(res_map)

    # Pass 3: Check for ANY missing ID or ANY line that still has English letters [A-Za-z]
    for retry_round in range(2):
        stragglers = []
        for item in segments:
            sid = item["id"]
            fa_text = combined_map.get(sid, "")
            if not fa_text or has_english(fa_text):
                stragglers.append(item)

        if not stragglers:
            break

        print(f"Pass 3 Sweep (Round {retry_round + 1}): Fixing {len(stragglers)} lines that had English or were missed...")
        swept_map = await loop.run_in_executor(
            None,
            force_translate_stragglers,
            stragglers,
            context_info,
            gemini_models,
            groq_models
        )
        combined_map.update(swept_map)

    translated_all = []
    for item in segments:
        fa_line = combined_map.get(item["id"], "")
        # Final safety cleanup: strip any stray Latin word if an LLM still slipped one in
        if not fa_line:
            fa_line = "..."
        translated_all.append({
            "start": item["start"],
            "end": item["end"],
            "text": fa_line
        })

    return translated_all


# ================= SUBTITLE (.ASS) & FFMPEG RENDERING =================
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


async def burn_subtitles_ffmpeg_async(
    client: TelegramClient,
    input_video: str,
    ass_path: str,
    fonts_dir: str,
    output_video: str,
    target_w: int,
    target_h: int,
    total_duration: int
):
    vf_filter = f"scale={target_w}:{target_h}:flags=fast_bilinear,ass={ass_path}:fontsdir={fonts_dir}"
    cmd = [
        "ffmpeg", "-y", "-threads", "0", "-i", input_video,
        "-vf", vf_filter,
        "-c:v", "libx264", "-preset", "ultrafast", "-tune", "fastdecode", "-crf", "25",
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
                    f"🎬 <b>مرحله ۴ از ۵:</b> رندر فوق‌سریع زیرنویس...\n"
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
    client = TelegramClient(
        None,
        API_ID,
        API_HASH,
        flood_sleep_threshold=60
    )
    await client.start(bot_token=BOT_TOKEN)

    try:
        async def dl_progress(current, total):
            if total > 0:
                pct = (current / total) * 100.0
                mb_cur = current / (1024 * 1024)
                mb_tot = total / (1024 * 1024)
                bar = make_progress_bar(pct)
                text = (
                    f"📥 <b>مرحله ۱ از ۵:</b> دانلود موازی ۸ کاناله از تلگرام...\n"
                    f"{bar} <b>{pct:.1f}%</b> (<code>{mb_cur:.1f} / {mb_tot:.1f} MB</code>)"
                )
                await update_status(client, text, force=False)

        # 1. Parallel 8-Socket Download
        await update_status(client, "📥 <b>مرحله ۱ از ۵:</b> شروع دانلود موازی پرسرعت...", force=True)
        video_msg = await client.get_messages(CHAT_ID, ids=VIDEO_MSG_ID)
        input_video = "input_video.mp4"
        await parallel_download_file(client, video_msg, input_video, progress_callback=dl_progress, workers=8)

        thumb_path = None
        if THUMB_MSG_ID and THUMB_MSG_ID > 0:
            thumb_msg = await client.get_messages(CHAT_ID, ids=THUMB_MSG_ID)
            if thumb_msg and thumb_msg.photo:
                thumb_path = await client.download_media(thumb_msg, file="custom_thumb.jpg")

        orig_w, orig_h, duration = get_video_info(input_video)
        target_w, target_h = compute_target_resolution(orig_w, orig_h)

        # 2. Extract Audio & Transcribe with Groq Whisper
        await update_status(client, "🎙 <b>مرحله ۲ از ۵:</b> استخراج صدا و تشخیص گفتار (Groq Whisper)...", force=True)
        audio_path = "extracted_audio.mp3"
        extract_compressed_audio(input_video, audio_path)
        segments = transcribe_audio_with_groq(audio_path)

        if not segments:
            await update_status(client, "⚠️ گفتگوی انگلیسی تشخیص داده نشد، در حال درج تگ...", force=True)
            translated_subs = []
        else:
            # 3. Three-Pass 100% Persian Translation
            await update_status(
                client,
                f"🧠 <b>مرحله ۳ از ۵:</b> تصحیح اسامی و ترجمه ۱۰۰٪ فارسی ({len(segments)} خط)...",
                force=True
            )
            translated_subs = await translate_segments_parallel(segments)

        # 4. Generate ASS & Burn with Ultra-Fast 720p FFmpeg
        await update_status(client, "🎬 <b>مرحله ۴ از ۵:</b> شروع هاردساب فوق‌سریع زیرنویس زرد...", force=True)
        ass_path = "subtitles.ass"
        fonts_dir = "./fonts"
        output_video = "output_subtitled.mp4"

        generate_ass_file(translated_subs, USER_TAG, target_w, target_h, ass_path)
        await burn_subtitles_ffmpeg_async(
            client, input_video, ass_path, fonts_dir, output_video, target_w, target_h, duration
        )

        if not thumb_path:
            thumb_path = "auto_thumb.jpg"
            subprocess.run([
                "ffmpeg", "-y", "-i", output_video, "-ss", "00:00:01",
                "-vframes", "1", "-q:v", "2", thumb_path
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if not os.path.exists(thumb_path):
                thumb_path = None

        # 5. Parallel 8-Socket Upload (With ZERO extra text/caption under the video)
        out_size_mb = os.path.getsize(output_video) / (1024 * 1024)
        await update_status(
            client,
            f"📤 <b>مرحله ۵ از ۵:</b> آپلود موازی ۸ کاناله (<code>{out_size_mb:.1f} MB</code>)...",
            force=True
        )

        async def ul_progress(current, total):
            if total > 0:
                pct = (current / total) * 100.0
                mb_cur = current / (1024 * 1024)
                mb_tot = total / (1024 * 1024)
                bar = make_progress_bar(pct)
                text = (
                    f"📤 <b>مرحله ۵ از ۵:</b> آپلود پرسرعت ویدیوی نهایی...\n"
                    f"{bar} <b>{pct:.1f}%</b> (<code>{mb_cur:.1f} / {mb_tot:.1f} MB</code>)"
                )
                await update_status(client, text, force=False)

        uploaded_input_file = await parallel_upload_file(
            client, output_video, progress_callback=ul_progress, workers=8
        )

        # Send ONLY the video with no caption at all
        await client.send_file(
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
            await client.delete_messages(CHAT_ID, STATUS_MSG_ID)
        except Exception:
            pass

    except Exception as e:
        error_msg = (
            f"❌ <b>متاسفانه خطایی در حین پردازش رخ داد:</b>\n"
            f"<code>{str(e)[:300]}</code>"
        )
        try:
            await client.edit_message(CHAT_ID, STATUS_MSG_ID, error_msg, parse_mode="html")
        except Exception as edit_err:
            print(f"Could not edit status message with error: {edit_err}")
        raise e
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
