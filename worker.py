import os
import sys
import json
import math
import asyncio
import subprocess
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

CHAT_ID = int(os.getenv("CHAT_ID") or "0")
STATUS_MSG_ID = int(os.getenv("STATUS_MSG_ID") or "0")
VIDEO_MSG_ID = int(os.getenv("VIDEO_MSG_ID") or "0")
THUMB_MSG_ID = int(os.getenv("THUMB_MSG_ID") or "0")
USER_TAG = os.getenv("USER_TAG") or "@YourChannelTag"

groq_client = Groq(api_key=GROQ_API_KEY)
gemini_client = genai.Client(api_key=GEMINI_API_KEY)

async def update_status(app: Client, step_text: str):
    try:
        text = (
            "⚙️ <b>در حال پردازش هوشمند ویدیو...</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"{step_text}\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "⏳ <i>لطفاً صبور باشید، عملیات به صورت خودکار انجام می‌شود.</i>"
        )
        await app.edit_message_text(
            chat_id=CHAT_ID,
            message_id=STATUS_MSG_ID,
            text=text,
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        print(f"Status update skipped: {e}")

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

def extract_compressed_audio(video_path: str, audio_path: str):
    """Extracts mono 16kHz 32k MP3 so even 1-hour videos stay under Groq's 25MB limit."""
    cmd = [
        "ffmpeg", "-y", "-i", video_path,
        "-vn", "-ac", "1", "-ar", "16000", "-b:a", "32k",
        audio_path
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def transcribe_audio_with_groq(audio_path: str) -> list[dict]:
    """Uses Whisper-large-v3-turbo with precise segment timestamps."""
    with open(audio_path, "rb") as f:
        transcription = groq_client.audio.transcriptions.create(
            file=(audio_path, f.read()),
            model="whisper-large-v3-turbo",
            response_format="verbose_json",
            language="en",
            temperature=0.0
        )

    raw_segments = getattr(transcription, "segments", []) or []
    clean_segments = []

    for seg in raw_segments:
        # Handle dict or object attributes from Groq SDK
        start = seg["start"] if isinstance(seg, dict) else seg.start
        end = seg["end"] if isinstance(seg, dict) else seg.end
        text = (seg["text"] if isinstance(seg, dict) else seg.text).strip()
        no_speech = (seg.get("no_speech_prob", 0.0) if isinstance(seg, dict) else getattr(seg, "no_speech_prob", 0.0))

        # Cut out silence / hallucinations so subtitles vanish when nobody speaks
        if not text or no_speech > 0.65:
            continue

        clean_segments.append({
            "id": len(clean_segments),
            "start": round(float(start), 2),
            "end": round(float(end), 2),
            "text": text
        })

    return clean_segments

def translate_segments_to_persian(segments: list[dict]) -> list[dict]:
    """Translates English subtitle segments into natural conversational Persian in batches."""
    if not segments:
        return []

    translated_all = []
    batch_size = 35

    system_instruction = (
        "You are an expert Iranian movie and social media subtitle translator. "
        "Translate the provided JSON array of English subtitle lines into natural, fluent, "
        "conversational Persian (فارسی محاوره‌ای و طبیعی). "
        "Rules:\n"
        "1. Keep each Persian line concise so it reads smoothly as a video subtitle.\n"
        "2. Translate idioms and slang naturally into Persian equivalents, NEVER word-for-word robotic translation.\n"
        "3. Return ONLY a valid JSON object with a single key 'translations' containing the array of objects "
        "with the exact same 'id' and a 'fa' field containing the Persian translation."
    )

    for i in range(0, len(segments), batch_size):
        batch = segments[i:i + batch_size]
        prompt_payload = json.dumps(
            [{"id": item["id"], "en": item["text"]} for item in batch],
            ensure_ascii=False
        )

        raw_json_text = None

        # 1. Try Gemini 3.8 Flash first
        if GEMINI_API_KEY:
            try:
                response = gemini_client.models.generate_content(
                    model="gemini-3.8-flash",
                    contents=prompt_payload,
                    config=types.GenerateContentConfig(
                        system_instruction=system_instruction,
                        response_mime_type="application/json",
                        temperature=0.2
                    )
                )
                raw_json_text = response.text
            except Exception as gemini_err:
                print(f"Gemini error, falling back to Groq LLM: {gemini_err}")

        # 2. Automatic Fallback to Groq (Llama 3.3 70B) if Gemini fails
        if not raw_json_text:
            chat_completion = groq_client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": system_instruction},
                    {"role": "user", "content": prompt_payload}
                ],
                response_format={"type": "json_object"},
                temperature=0.2
            )
            raw_json_text = chat_completion.choices[0].message.content

        try:
            parsed = json.loads(raw_json_text)
            fa_list = parsed.get("translations", parsed) if isinstance(parsed, dict) else parsed
            fa_map = {int(item["id"]): item["fa"] for item in fa_list if "id" in item and "fa" in item}
        except Exception as e:
            print(f"JSON parse fallback on batch {i}: {e}")
            fa_map = {}

        for item in batch:
            persian_text = fa_map.get(item["id"], item["text"])
            translated_all.append({
                "start": item["start"],
                "end": item["end"],
                "text": persian_text
            })

    return translated_all

def format_ass_time(seconds: float) -> str:
    """Converts seconds to ASS timestamp format H:MM:SS.cc"""
    hrs = int(seconds // 3600)
    mins = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    centis = int(round((seconds - int(seconds)) * 100))
    if centis >= 100:
        centis = 99
    return f"{hrs}:{mins:02d}:{secs:02d}.{centis:02d}"

def generate_ass_file(subtitles: list[dict], user_tag: str, width: int, height: int, ass_path: str):
    """
    Generates an .ass file scaled dynamically to the video resolution (Portrait Reels or Landscape 16:9).
    - Yellow Persian subtitles (&H0000FFFF) sit cleanly above the bottom watermark tag.
    """
    # Scale font sizes dynamically based on video resolution
    min_dim = min(width, height)
    sub_font_size = max(24, int(min_dim * 0.046))   # Clean, small-medium readable size
    tag_font_size = max(18, int(min_dim * 0.032))   # Subtle watermark tag at very bottom
    sub_margin_v = int(height * 0.085)              # Positioned safely above the bottom tag
    tag_margin_v = int(height * 0.022)              # Right near the bottom edge
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

    # Persistent bottom tag across the entire video (up to 10 hours)
    clean_tag = user_tag.replace("\n", " ").replace("{", "").replace("}", "")
    lines.append(f"Dialogue: 0,0:00:00.00,9:59:59.00,BottomTag,,0,0,0,,{clean_tag}")

    # Synced yellow Persian subtitles (disappear completely during silence)
    for sub in subtitles:
        start_str = format_ass_time(sub["start"])
        end_str = format_ass_time(sub["end"])
        clean_text = sub["text"].replace("\n", "\\N").replace("{", "").replace("}", "")
        # Force RTL embedding (\u202B) so mixed numbers/punctuation render in proper Persian order
        rtl_text = f"\u202B{clean_text}"
        lines.append(f"Dialogue: 1,{start_str},{end_str},YellowSub,,0,0,0,,{rtl_text}")

    with open(ass_path, "w", encoding="utf-8-sig") as f:
        f.write("\n".join(lines) + "\n")

def burn_subtitles_ffmpeg(input_video: str, ass_path: str, fonts_dir: str, output_video: str):
    """Burns the .ass subtitles and watermark tag using hardware-friendly libx264 settings."""
    vf_filter = f"ass={ass_path}:fontsdir={fonts_dir}"
    cmd = [
        "ffmpeg", "-y", "-i", input_video,
        "-vf", vf_filter,
        "-c:v", "libx264", "-preset", "superfast", "-crf", "23",
        "-c:a", "aac", "-b:a", "128k",
        "-movflags", "+faststart",
        output_video
    ]
    subprocess.run(cmd, check=True)

async def main():
    app = Client(
        "worker_session",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
        in_memory=True
    )

    async with app:
        try:
            # 1. Download Video (and optional custom thumbnail)
            await update_status(app, "📥 <b>مرحله ۱ از ۵:</b> در حال دانلود ویدیو با سرعت بالا...")
            video_msg = await app.get_messages(CHAT_ID, VIDEO_MSG_ID)
            input_video = await app.download_media(video_msg, file_name="input_video.mp4")

            thumb_path = None
            if THUMB_MSG_ID and THUMB_MSG_ID > 0:
                thumb_msg = await app.get_messages(CHAT_ID, THUMB_MSG_ID)
                if thumb_msg and thumb_msg.photo:
                    thumb_path = await app.download_media(thumb_msg, file_name="custom_thumb.jpg")

            width, height, duration = get_video_info(input_video)

            # 2. Extract Audio & Transcribe with Groq Whisper
            await update_status(app, "🎙 <b>مرحله ۲ از ۵:</b> استخراج صدا و تشخیص زمان‌بندی دقیق جملات...")
            audio_path = "extracted_audio.mp3"
            extract_compressed_audio(input_video, audio_path)
            segments = transcribe_audio_with_groq(audio_path)

            if not segments:
                await update_status(app, "⚠️ هیچ گفتگوی انگلیسی در این ویدیو تشخیص داده نشد، در حال درج تگ و آماده‌سازی...")
                translated_subs = []
            else:
                # 3. Contextual Persian Translation with Gemini
                await update_status(
                    app,
                    f"🧠 <b>مرحله ۳ از ۵:</b> در حال ترجمه هوشمند و محاوره‌ای ({len(segments)} بخش گفتاری)..."
                )
                translated_subs = translate_segments_to_persian(segments)

            # 4. Generate ASS & Burn with FFmpeg
            await update_status(app, "🎬 <b>مرحله ۴ از ۵:</b> در حال هاردساب زیرنویس زرد و درج تگ روی ویدیو...")
            ass_path = "subtitles.ass"
            fonts_dir = "./fonts"
            output_video = "output_subtitled.mp4"

            generate_ass_file(translated_subs, USER_TAG, width, height, ass_path)
            burn_subtitles_ffmpeg(input_video, ass_path, fonts_dir, output_video)

            # If user didn't send a custom thumbnail, extract a clean frame at 1s so Telegram displays a nice preview
            if not thumb_path:
                thumb_path = "auto_thumb.jpg"
                subprocess.run([
                    "ffmpeg", "-y", "-i", output_video, "-ss", "00:00:01",
                    "-vframes", "1", "-q:v", "2", thumb_path
                ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                if not os.path.exists(thumb_path):
                    thumb_path = None

            # 5. Upload Final Video to User
            await update_status(app, "📤 <b>مرحله ۵ از ۵:</b> در حال آپلود ویدیوی نهایی در تلگرام...")

            caption = (
                "✅ <b>ویدیوی شما با موفقیت آماده شد!</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"🏷 <b>تگ درج شده:</b> <code>{USER_TAG}</code>\n"
                f"💬 <b>تعداد دیالوگ‌های ترجمه شده:</b> {len(translated_subs)} خط\n"
                "✨ <i>ترجمه و زیرنویس اختصاصی با هوش مصنوعی</i>"
            )

            await app.send_video(
                chat_id=CHAT_ID,
                video=output_video,
                caption=caption,
                thumb=thumb_path,
                width=width,
                height=height,
                duration=duration,
                supports_streaming=True,
                reply_to_message_id=VIDEO_MSG_ID,
                parse_mode=ParseMode.HTML
            )

            await app.delete_messages(CHAT_ID, STATUS_MSG_ID)

        except Exception as e:
            error_msg = (
                f"❌ <b>متاسفانه خطایی در حین پردازش رخ داد:</b>\n"
                f"<code>{str(e)[:300]}</code>"
            )
            await app.edit_message_text(
                chat_id=CHAT_ID,
                message_id=STATUS_MSG_ID,
                text=error_msg,
                parse_mode=ParseMode.HTML
            )
            raise e

if __name__ == "__main__":
    asyncio.run(main())
