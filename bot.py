import os
import html
import asyncio
import aiohttp
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.filters import CommandStart, Command
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

# ================= ENVIRONMENT VARIABLES =================
BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
GITHUB_PAT = (os.getenv("GITHUB_PAT") or "").strip()
GH_OWNER = (os.getenv("GH_OWNER") or "").strip()
GH_REPO = (os.getenv("GH_REPO") or "").strip()
DEFAULT_TAG = (os.getenv("DEFAULT_TAG") or "@YourChannelTag").strip()
PORT = int(os.getenv("PORT", 10000))
RENDER_URL = os.getenv("RENDER_EXTERNAL_URL") or f"https://{GH_REPO}.onrender.com"

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is missing!")

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# Shared HTTP session for low-latency GitHub API dispatches
http_client: aiohttp.ClientSession | None = None

# In-memory session store: {user_id: {...}}
user_sessions: dict[int, dict] = {}


def get_session(user_id: int) -> dict:
    if user_id not in user_sessions:
        user_sessions[user_id] = {
            "video_msg_id": None,
            "video_file_name": "ویدیوی ارسالی",
            "thumb_msg_id": None,
            "panel_msg_id": None,
            "tag": DEFAULT_TAG,
            "awaiting_input": None,  # "tag" or "thumb"
            "is_dispatching": False,
        }
    return user_sessions[user_id]


def build_panel_keyboard(session: dict) -> InlineKeyboardMarkup:
    has_thumb = "✅ تنظیم شد" if session["thumb_msg_id"] else "➕ افزودن (اختیاری)"
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=f"🖼 تامنیل: {has_thumb}", callback_data="set_thumb"),
            InlineKeyboardButton(text=f"🏷 تگ: {session['tag']}", callback_data="set_tag")
        ],
        [
            InlineKeyboardButton(text="🎬 شروع ساخت زیرنویس هوشمند", callback_data="start_render")
        ],
        [
            InlineKeyboardButton(text="❌ لغو عملیات", callback_data="cancel_job")
        ]
    ])


def build_panel_text(session: dict, file_name: str | None = None) -> str:
    display_name = file_name or session.get("video_file_name") or "ویدیوی ارسالی"
    safe_name = html.escape(display_name)
    safe_tag = html.escape(session["tag"])
    thumb_status = "✅ دریافت شده" if session["thumb_msg_id"] else "➖ بدون تامنیل سفارشی"
    return (
        "💎 <b>پنل هوشمند زیرنویس و ادیت ویدیو (نسخه توربو پرو)</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"📹 <b>فایل ویدیو:</b> <code>{safe_name}</code>\n"
        f"🖼 <b>کاور (تامنیل):</b> {thumb_status}\n"
        f"🏷 <b>تگ پایین ویدیو:</b> <code>{safe_tag}</code>\n"
        "🎨 <b>استایل زیرنویس:</b> فونت فارسی زرد (سینمایی هماهنگ با صدا)\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "👇 در صورت تمایل تامنیل یا تگ را تغییر دهید، یا مستقیم دکمه <b>شروع</b> را بزنید:"
    )


async def refresh_panel_message(message: Message, session: dict, prefix_notice: str = ""):
    """Deletes outdated panel (if any) and sends an updated panel preserving the video filename."""
    old_panel_id = session.get("panel_msg_id")
    if old_panel_id:
        try:
            await bot.delete_message(chat_id=message.chat.id, message_id=old_panel_id)
        except Exception:
            pass

    full_text = f"{prefix_notice}\n\n{build_panel_text(session)}" if prefix_notice else build_panel_text(session)
    sent = await message.reply(full_text, reply_markup=build_panel_keyboard(session))
    session["panel_msg_id"] = sent.message_id


# ================= HANDLERS =================

@dp.message(CommandStart())
async def cmd_start(message: Message):
    get_session(message.from_user.id)
    welcome_text = (
        "✨ <b>به ربات هوشمند ترجمه و زیرنویس ویدیو خوش آمدید!</b>\n\n"
        "🚀 <b>امکانات این سیستم:</b>\n"
        "• تشخیص دقیق صحبت‌های انگلیسی و قطع خودکار در لحظات سکوت\n"
        "• ترجمه روان و محاوره‌ای با هوش مصنوعی (بدون ترجمه ربات‌گونه)\n"
        "• هاردساب فونت فارسی زرد رنگ + درج تگ اختصاصی شما در پایین ویدیو\n"
        "• پشتیبانی از ویدیوهای حجیم و طولانی با حفظ کیفیت Full HD 1080p\n\n"
        "📥 <b>برای شروع، کافیست یک ویدیو بفرستید یا فوروارد کنید!</b>"
    )
    await message.answer(welcome_text)


@dp.message(Command("cancel"))
async def cmd_cancel(message: Message):
    session = get_session(message.from_user.id)
    session["video_msg_id"] = None
    session["video_file_name"] = "ویدیوی ارسالی"
    session["thumb_msg_id"] = None
    session["awaiting_input"] = None
    session["is_dispatching"] = False
    await message.answer("❌ عملیات جاری لغو شد. هر زمان آماده بودید ویدیوی جدیدی ارسال کنید.")


@dp.message(F.video | F.document | F.video_note)
async def handle_video(message: Message):
    if message.document and not (message.document.mime_type and message.document.mime_type.startswith("video/")):
        return await message.answer("⚠️ لطفاً یک فایل ویدیویی معتبر ارسال کنید.")

    session = get_session(message.from_user.id)
    session["video_msg_id"] = message.message_id
    session["awaiting_input"] = None
    session["is_dispatching"] = False

    file_name = "Video.mp4"
    if message.video and message.video.file_name:
        file_name = message.video.file_name
    elif message.document and message.document.file_name:
        file_name = message.document.file_name
    elif message.video_note:
        file_name = "VideoNote.mp4"

    session["video_file_name"] = file_name
    await refresh_panel_message(message, session)


@dp.message(F.photo)
async def handle_photo(message: Message):
    session = get_session(message.from_user.id)
    session["thumb_msg_id"] = message.message_id
    session["awaiting_input"] = None

    if session["video_msg_id"]:
        await refresh_panel_message(message, session, prefix_notice="✅ <b>تامنیل اختصاصی شما ثبت شد!</b>")
    else:
        await message.reply("✅ تامنیل ذخیره شد! حالا ویدیوی خود را ارسال یا فوروارد کنید.")


@dp.message(F.text)
async def handle_text(message: Message):
    session = get_session(message.from_user.id)
    if session.get("awaiting_input") == "tag":
        new_tag = message.text.strip()[:40]
        if not new_tag:
            return await message.reply("⚠️ تگ نمی‌تواند خالی باشد. لطفاً متن تگ را ارسال کنید:")
        session["tag"] = new_tag
        session["awaiting_input"] = None
        safe_tag = html.escape(session["tag"])
        if session["video_msg_id"]:
            await refresh_panel_message(
                message,
                session,
                prefix_notice=f"✅ تگ شما به <code>{safe_tag}</code> تغییر یافت!"
            )
        else:
            await message.reply(f"✅ تگ <code>{safe_tag}</code> ذخیره شد. اکنون ویدیو را بفرستید.")
    else:
        await message.reply("📥 لطفاً یک <b>ویدیو</b> ارسال یا فوروارد کنید تا پنل تنظیمات باز شود.")


# ================= CALLBACKS =================

@dp.callback_query(F.data == "set_thumb")
async def cb_set_thumb(call: CallbackQuery):
    session = get_session(call.from_user.id)
    session["awaiting_input"] = "thumb"
    await call.message.answer("🖼 لطفاً عکس <b>تامنیل (کاور)</b> مورد نظر خود را همینجا ارسال کنید:")
    await call.answer()


@dp.callback_query(F.data == "set_tag")
async def cb_set_tag(call: CallbackQuery):
    session = get_session(call.from_user.id)
    session["awaiting_input"] = "tag"
    await call.message.answer("🏷 لطفاً متن <b>تگ پایین ویدیو</b> را ارسال کنید (مثال: <code>@MyChannel</code>):")
    await call.answer()


@dp.callback_query(F.data == "cancel_job")
async def cb_cancel_job(call: CallbackQuery):
    session = get_session(call.from_user.id)
    session["video_msg_id"] = None
    session["video_file_name"] = "ویدیوی ارسالی"
    session["thumb_msg_id"] = None
    session["panel_msg_id"] = None
    session["awaiting_input"] = None
    session["is_dispatching"] = False
    await call.message.edit_text("❌ عملیات لغو شد. هر زمان آماده بودید ویدیوی جدیدی ارسال کنید.")
    await call.answer()


@dp.callback_query(F.data == "start_render")
async def cb_start_render(call: CallbackQuery):
    session = get_session(call.from_user.id)
    if session.get("is_dispatching"):
        return await call.answer("⏳ درخواست شما در حال ارسال است، لطفاً صبر کنید...", show_alert=False)

    if not session["video_msg_id"]:
        return await call.answer("⚠️ ابتدا یک ویدیو ارسال کنید!", show_alert=True)

    # Lock immediately to prevent duplicate double-click dispatches
    session["is_dispatching"] = True
    video_msg_id = session["video_msg_id"]
    thumb_msg_id = session["thumb_msg_id"] or 0
    user_tag = session["tag"]

    await call.answer("🚀 در حال ارسال به سرور پردازش...")
    status_msg = await call.message.edit_text(
        "⚙️ <b>درخواست شما در صف پردازش ابری قرار گرفت!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "🔄 <b>وضعیت:</b> در حال راه‌اندازی سرور پردازشگر توربو...\n"
        "⏳ <i>این پیام به صورت زنده وضعیت پیشرفت را نمایش می‌دهد.</i>"
    )

    success = await trigger_github_workflow(
        chat_id=call.message.chat.id,
        status_msg_id=status_msg.message_id,
        video_msg_id=video_msg_id,
        thumb_msg_id=thumb_msg_id,
        user_tag=user_tag
    )

    session["is_dispatching"] = False

    if not success:
        await status_msg.edit_text(
            "❌ <b>خطا در ارتباط با سرور پردازش (GitHub Actions)!</b>\n"
            "لطفاً تنظیمات توکن گیت‌هاب (GITHUB_PAT / GH_OWNER / GH_REPO) را بررسی کنید.",
            reply_markup=build_panel_keyboard(session)
        )
    else:
        # Reset video & thumb for next job while keeping their custom tag
        session["video_msg_id"] = None
        session["video_file_name"] = "ویدیوی ارسالی"
        session["thumb_msg_id"] = None
        session["panel_msg_id"] = None


async def trigger_github_workflow(
    chat_id: int,
    status_msg_id: int,
    video_msg_id: int,
    thumb_msg_id: int,
    user_tag: str
) -> bool:
    global http_client
    url = f"https://api.github.com/repos/{GH_OWNER}/{GH_REPO}/actions/workflows/process_video.yml/dispatches"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {GITHUB_PAT}",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    payload = {
        "ref": "main",
        "inputs": {
            "chat_id": str(chat_id),
            "status_msg_id": str(status_msg_id),
            "video_msg_id": str(video_msg_id),
            "thumb_msg_id": str(thumb_msg_id),
            "user_tag": user_tag
        }
    }
    try:
        session = http_client or aiohttp.ClientSession()
        async with session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 204:
                body = await resp.text()
                print(f"[Workflow Dispatch Error] HTTP {resp.status}: {body}", flush=True)
                return False
            return True
    except Exception as e:
        print(f"[Workflow Dispatch Exception] {e}", flush=True)
        return False


# ================= RENDER KEEP-ALIVE WEB SERVER =================

async def health_check(request):
    return web.Response(text="Bot is alive and running!")


async def render_keep_awake():
    """Periodically pings the Render public endpoint every 10 minutes."""
    global http_client
    await asyncio.sleep(90)
    while True:
        try:
            if http_client:
                async with http_client.get(RENDER_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    print(f"[Keep-Awake] Pinged {RENDER_URL} -> HTTP {r.status}", flush=True)
        except Exception as e:
            print(f"[Keep-Awake] Note: {e}", flush=True)
        await asyncio.sleep(600)


async def main():
    global http_client
    http_client = aiohttp.ClientSession()

    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"Keep-alive server started on port {PORT}", flush=True)

    asyncio.create_task(render_keep_awake())

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await http_client.close()


if __name__ == "__main__":
    asyncio.run(main())
