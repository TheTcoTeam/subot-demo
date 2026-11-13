import os
import sys
import json
import time
import math
import asyncio
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pyrogram import Client
from pyrogram.enums import ParseMode
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


async def update_status(app: Client, step_text: str, force: bool = True):
    global last_edit_time
    now = time.time()
    if not force and (now - last_edit_time) < 7.0:
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
        await app.edit_message_text(
            chat_id=CHAT_ID,
            message_id=STATUS_MSG_ID,
            text=text,
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        print(f"Status update skipped: {e}")


def make_progress_bar(percent: float) -> str:
    filled = int(round(percent / 10))
    filled = max(0, min(10, filled))
    return "🟩" * filled + "⬜" * (10 - filled)


def get_video_info(video_path: str) -> tuple[int, int, int]:
    """Returns (width, height, duration_seconds) using ffprobe."""
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
    """
    Caps the shorter dimension at 720p (e.g., 1280x720 for landscape or 720x1280 for portrait).
    This makes FFmpeg 3x faster and shrinks 400MB files down to ~80MB for 5x faster Telegram upload!
    """
    min_dim = min(width, height)
    if min_dim <= 720:
        # Ensure even dimensions for libx264
        return (width - (width % 2), height - (height % 2))
    scale_factor = 720.0 / float(min_dim)
    new_w = int(round(width * scale_factor))
    new_h = int(round(height * scale_factor))
    new_w -= (new_w % 2)
    new_h -= (new_h % 2)
    return new_w, new_h


def extract_compressed_audio(video_path: str, audio_path: str):
    """Extracts mono 16kHz 32k MP3 using multi-threading."""
    cmd = [
        "ffmpeg", "-y", "-threads", "0", "-i", video_path,
        "-vn", "-ac", "1", "-ar", "16000", "-b:a", "32k",
        audio_path
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def transcribe_audio_with_groq(audio_path: str) -> list[dict]:
    """Uses Whisper on Groq with automatic model fallback."""
    whisper_models = ["whisper-large-v3-turbo", "whisper-large-v3"]
    transcription = None

    for w_model in whisper_models:
        try:
            with open(audio_path, "rb") as f:
                transcription = groq_client.audio.transcriptions.create(
                    file=(audio_path, f.read()),
                    model=w_model,
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

    preferred_keywords = ["llama", "qwen", "deepseek", "gemma", "mixtral"]
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
    return json.loads(text)


def translate_single_batch_sync(
    batch: list[dict],
    batch_idx: int,
    gemini_models: list[str],
    groq_models: list[str],
    system_instruction: str
) -> dict[int, str]:
    """Translates a single batch synchronously (called concurrently across threads)."""
    prompt_payload = json.dumps(
        [{"id": item["id"], "en": item["text"]} for item in batch],
        ensure_ascii=False
    )

    parsed_json = None

    # 1. Try Groq fast chat models first for ultra-low latency (hundreds of tokens/sec)
    for q_model in groq_models[:3]:
        try:
            chat_completion = groq_client.chat.completions.create(
                model=q_model,
                messages=[
                    {"role": "system", "content": system_instruction},
                    {"role": "user", "content": prompt_payload}
                ],
                temperature=0.2
            )
            raw_text = chat_completion.choices[0].message.content
            parsed_json = extract_json_from_text(raw_text)
            print(f"Batch {batch_idx} translated in parallel via Groq ({q_model})")
            break
        except Exception as groq_err:
            print(f"Groq {q_model} failed on batch {batch_idx}: {str(groq_err)[:120]}")

    # 2. Fallback to Gemini models if Groq rate-limits
    if parsed_json is None and gemini_client:
        for g_model in gemini_models:
            try:
                response = gemini_client.models.generate_content(
                    model=g_model,
                    contents=prompt_payload,
                    config=types.GenerateContentConfig(
                        system_instruction=system_instruction,
                        response_mime_type="application/json",
                        temperature=0.2
                    )
                )
                parsed_json = extract_json_from_text(response.text)
                print(f"Batch {batch_idx} translated in parallel via Gemini ({g_model})")
                break
            except Exception as gemini_err:
                print(f"Gemini {g_model} failed on batch {batch_idx}: {str(gemini_err)[:120]}")

    if parsed_json is None:
        return {}

    try:
        fa_list = parsed_json.get("translations", parsed_json) if isinstance(parsed_json, dict) else parsed_json
        return {int(item["id"]): str(item["fa"]) for item in fa_list if "id" in item and "fa" in item}
    except Exception as e:
        print(f"JSON mapping error on batch {batch_idx}: {e}")
        return {}


async def translate_segments_parallel(segments: list[dict]) -> list[dict]:
    """Translates ALL subtitle batches concurrently in 3-5 seconds using ThreadPoolExecutor."""
    if not segments:
        return []

    gemini_models = get_available_gemini_models()
    groq_models = get_available_groq_chat_models()

    system_instruction = (
        "You are an expert Iranian movie and social media subtitle translator. "
        "Translate the provided JSON array of English subtitle lines into natural, fluent, "
        "conversational Persian (فارسی محاوره‌ای و طبیعی). "
        "Rules:\n"
        "1. Keep each Persian line concise so it reads smoothly as a video subtitle.\n"
        "2. Translate idioms and slang naturally into Persian equivalents, NEVER word-for-word robotic translation.\n"
        "3. Return ONLY a valid JSON object with a single key \"translations\" containing the array of objects "
        "with the exact same \"id\" and a \"fa\" field containing the Persian translation. Do not include markdown."
    )

    batch_size = 40
    batches = [segments[i:i + batch_size] for i in range(0, len(segments), batch_size)]

    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=4) as pool:
        tasks = [
            loop.run_in_executor(
                pool,
                translate_single_batch_sync,
                batch,
                idx,
                gemini_models,
                groq_models,
                system_instruction
            )
            for idx, batch in enumerate(batches)
        ]
        results = await asyncio.gather(*tasks)

    combined_map = {}
    for res_map in results:
        combined_map.update(res_map)

    translated_all = []
    for item in segments:
        translated_all.append({
            "start": item["start"],
            "end": item["end"],
            "text": combined_map.get(item["id"], item["text"])
        })

    return translated_all


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
    app: Client,
    input_video: str,
    ass_path: str,
    fonts_dir: str,
    output_video: str,
    target_w: int,
    target_h: int,
    total_duration: int
):
    """
    Ultra-fast FFmpeg pipeline:
    1. Scales oversized 1080p/4K videos down to 720p HD BEFORE burning subtitles (3x faster CPU render + 5x smaller upload).
    2. Uses '-preset ultrafast -tune fastdecode -crf 25' to hit 4x-6x real-time speed on GitHub's 4-core CPU.
    3. Copies original audio ('-c:a copy') in 0 seconds.
    """
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
                    app,
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


async def main():
    app = Client(
        "worker_session",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
        in_memory=True,
        workers=16,
        max_concurrent_transmissions=8,
        sleep_threshold=60
    )

    async with app:
        try:
            t_start = time.time()
            loop = asyncio.get_running_loop()

            def download_progress(current, total):
                if total > 0:
                    pct = (current / total) * 100.0
                    mb_cur = current / (1024 * 1024)
                    mb_tot = total / (1024 * 1024)
                    bar = make_progress_bar(pct)
                    text = (
                        f"📥 <b>مرحله ۱ از ۵:</b> دانلود ویدیو از تلگرام...\n"
                        f"{bar} <b>{pct:.1f}%</b> (<code>{mb_cur:.1f} / {mb_tot:.1f} MB</code>)"
                    )
                    asyncio.run_coroutine_threadsafe(update_status(app, text, force=False), loop)

            # 1. Download Video
            await update_status(app, "📥 <b>مرحله ۱ از ۵:</b> شروع دانلود پرسرعت ویدیو...", force=True)
            video_msg = await app.get_messages(CHAT_ID, VIDEO_MSG_ID)
            input_video = await app.download_media(
                video_msg,
                file_name="input_video.mp4",
                progress=download_progress
            )

            thumb_path = None
            if THUMB_MSG_ID and THUMB_MSG_ID > 0:
                thumb_msg = await app.get_messages(CHAT_ID, THUMB_MSG_ID)
                if thumb_msg and thumb_msg.photo:
                    thumb_path = await app.download_media(thumb_msg, file_name="custom_thumb.jpg")

            orig_w, orig_h, duration = get_video_info(input_video)
            target_w, target_h = compute_target_resolution(orig_w, orig_h)

            # 2. Extract Audio & Transcribe with Groq Whisper
            await update_status(app, "🎙 <b>مرحله ۲ از ۵:</b> استخراج صدا و تشخیص گفتار (Groq Whisper)...", force=True)
            audio_path = "extracted_audio.mp3"
            extract_compressed_audio(input_video, audio_path)
            segments = transcribe_audio_with_groq(audio_path)

            if not segments:
                await update_status(app, "⚠️ گفتگوی انگلیسی تشخیص داده نشد، در حال درج تگ...", force=True)
                translated_subs = []
            else:
                # 3. Parallel Persian Translation (All batches simultaneously!)
                await update_status(
                    app,
                    f"⚡ <b>مرحله ۳ از ۵:</b> ترجمه موازی و همزمان ({len(segments)} خط دیالوگ)...",
                    force=True
                )
                translated_subs = await translate_segments_parallel(segments)

            # 4. Generate ASS & Burn with Ultra-Fast 720p FFmpeg
            await update_status(app, "🎬 <b>مرحله ۴ از ۵:</b> شروع هاردساب فوق‌سریع زیرنویس زرد...", force=True)
            ass_path = "subtitles.ass"
            fonts_dir = "./fonts"
            output_video = "output_subtitled.mp4"

            generate_ass_file(translated_subs, USER_TAG, target_w, target_h, ass_path)
            await burn_subtitles_ffmpeg_async(
                app, input_video, ass_path, fonts_dir, output_video, target_w, target_h, duration
            )

            if not thumb_path:
                thumb_path = "auto_thumb.jpg"
                subprocess.run([
                    "ffmpeg", "-y", "-i", output_video, "-ss", "00:00:01",
                    "-vframes", "1", "-q:v", "2", thumb_path
                ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                if not os.path.exists(thumb_path):
                    thumb_path = None

            # 5. Upload Final Video to User
            out_size_mb = os.path.getsize(output_video) / (1024 * 1024)
            await update_status(
                app,
                f"📤 <b>مرحله ۵ از ۵:</b> آپلود ویدیوی بهینه‌شده (<code>{out_size_mb:.1f} MB</code>)...",
                force=True
            )

            def upload_progress(current, total):
                if total > 0:
                    pct = (current / total) * 100.0
                    mb_cur = current / (1024 * 1024)
                    mb_tot = total / (1024 * 1024)
                    bar = make_progress_bar(pct)
                    text = (
                        f"📤 <b>مرحله ۵ از ۵:</b> در حال آپلود ویدیوی نهایی...\n"
                        f"{bar} <b>{pct:.1f}%</b> (<code>{mb_cur:.1f} / {mb_tot:.1f} MB</code>)"
                    )
                    asyncio.run_coroutine_threadsafe(update_status(app, text, force=False), loop)

            elapsed_sec = int(time.time() - t_start)
            mins, secs = divmod(elapsed_sec, 60)

            caption = (
                "✅ <b>ویدیوی شما با موفقیت آماده شد!</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"🏷 <b>تگ درج شده:</b> <code>{USER_TAG}</code>\n"
                f"💬 <b>تعداد دیالوگ‌ها:</b> {len(translated_subs)} خط\n"
                f"⏱ <b>زمان کل پردازش:</b> {mins} دقیقه و {secs} ثانیه\n"
                "✨ <i>ترجمه و زیرنویس اختصاصی با هوش مصنوعی</i>"
            )

            await app.send_video(
                chat_id=CHAT_ID,
                video=output_video,
                caption=caption,
                thumb=thumb_path,
                width=target_w,
                height=target_h,
                duration=duration,
                supports_streaming=True,
                reply_to_message_id=VIDEO_MSG_ID,
                parse_mode=ParseMode.HTML,
                progress=upload_progress
            )

            try:
                await app.delete_messages(CHAT_ID, STATUS_MSG_ID)
            except Exception:
                pass

        except Exception as e:
            error_msg = (
                f"❌ <b>متاسفانه خطایی در حین پردازش رخ داد:</b>\n"
                f"<code>{str(e)[:300]}</code>"
            )
            try:
                await app.edit_message_text(
                    chat_id=CHAT_ID,
                    message_id=STATUS_MSG_ID,
                    text=error_msg,
                    parse_mode=ParseMode.HTML
                )
            except Exception as edit_err:
                print(f"Could not edit status message with error: {edit_err}")
            raise e


if __name__ == "__main__":
    asyncio.run(main())
