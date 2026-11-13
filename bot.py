import os
import asyncio
import aiohttp
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.filters import CommandStart
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

# ================= ENVIRONMENT VARIABLES =================
BOT_TOKEN = os.getenv("BOT_TOKEN")
GITHUB_PAT = os.getenv("GITHUB_PAT")          # Personal Access Token (with repo & workflow scope)
GH_OWNER = os.getenv("GH_OWNER")              # Your GitHub username
GH_REPO = os.getenv("GH_REPO")                # Your GitHub repository name
DEFAULT_TAG = os.getenv("DEFAULT_TAG", "@YourChannelTag")
PORT = int(os.getenv("PORT", 10000))

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# In-memory session store for the demo: {user_id: {...}}
user_sessions = {}

def get_session(user_id: int) -> dict:
    if user_id not in user_sessions:
        user_sessions[user_id] = {
            "video_msg_id": None,
            "thumb_msg_id": None,
            "tag": DEFAULT_TAG,
            "awaiting_input": None  # "tag" or "thumb"
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

def build_panel_text(session: dict, file_name: str = "ویدیوی ارسالی") -> str:
    thumb_status = "✅ دریافت شده" if session["thumb_msg_id"] else "➖ بدون تامنیل سفارشی"
    return (
        "💎 <b>پنل هوشمند زیرنویس و ادیت ویدیو (نسخه دمو)</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"📹 <b>فایل ویدیو:</b> <code>{file_name}</code>\n"
        f"🖼 <b>کاور (تامنیل):</b> {thumb_status}\n"
        f"🏷 <b>تگ پایین ویدیو:</b> <code>{session['tag']}</code>\n"
        "🎨 <b>استایل زیرنویس:</b> فونت فارسی زرد (سینمایی هماهنگ با صدا)\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "👇 در صورت تمایل تامنیل یا تگ را تغییر دهید، یا مستقیم دکمه <b>شروع</b> را بزنید:"
    )

# ================= HANDLERS =================

@dp.message(CommandStart())
async def cmd_start(message: Message):
    get_session(message.from_user.id)
    welcome_text = (
        "✨ <b>به ربات هوشمند ترجمه و زیرنویس ویدیو خوش آمدید!</b>\n\n"
        "🚀 <b>امکانات این سیستم:</b>\n"
        "• تشخیص دقیق صحبت‌های انگلیسی و قطع خودکار در لحظات سکوت\n"
        "• ترجمه روان و محاوره‌ای با هوش مصنوعی (بدون ترجمه ربات گونه)\n"
        "• هاردساب فونت فارسی زرد رنگ + درج تگ اختصاصی شما در پایین ویدیو\n"
        "• پشتیبانی از ویدیوهای حجیم و طولانی\n\n"
        "📥 <b>برای شروع، کافیست یک ویدیو بفرستید یا فوروارد کنید!</b>"
    )
    await message.answer(welcome_text)

@dp.message(F.video | F.document)
async def handle_video(message: Message):
    # Ensure document is actually a video file if sent as document
    if message.document and not (message.document.mime_type and message.document.mime_type.startswith("video/")):
        return await message.answer("⚠️ لطفاً یک فایل ویدیویی معتبر ارسال کنید.")

    session = get_session(message.from_user.id)
    session["video_msg_id"] = message.message_id
    session["awaiting_input"] = None

    file_name = "Video.mp4"
    if message.video and message.video.file_name:
        file_name = message.video.file_name
    elif message.document and message.document.file_name:
        file_name = message.document.file_name

    await message.reply(
        build_panel_text(session, file_name),
        reply_markup=build_panel_keyboard(session)
    )

@dp.message(F.photo)
async def handle_photo(message: Message):
    session = get_session(message.from_user.id)
    session["thumb_msg_id"] = message.message_id
    session["awaiting_input"] = None

    if session["video_msg_id"]:
        await message.reply(
            "✅ <b>تامنیل اختصاصی شما ثبت شد!</b>\n\n" + build_panel_text(session),
            reply_markup=build_panel_keyboard(session)
        )
    else:
        await message.reply("✅ تامنیل ذخیره شد! حالا ویدیوی خود را ارسال یا فوروارد کنید.")

@dp.message(F.text)
async def handle_text(message: Message):
    session = get_session(message.from_user.id)
    if session.get("awaiting_input") == "tag":
        session["tag"] = message.text.strip()[:40]
        session["awaiting_input"] = None
        if session["video_msg_id"]:
            await message.reply(
                f"✅ تگ شما به <code>{session['tag']}</code> تغییر یافت!\n\n" + build_panel_text(session),
                reply_markup=build_panel_keyboard(session)
            )
        else:
            await message.reply(f"✅ تگ <code>{session['tag']}</code> ذخیره شد. اکنون ویدیو را بفرستید.")
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
    session["thumb_msg_id"] = None
    session["awaiting_input"] = None
    await call.message.edit_text("❌ عملیات لغو شد. هر زمان آماده بودید ویدیوی جدیدی ارسال کنید.")
    await call.answer()

@dp.callback_query(F.data == "start_render")
async def cb_start_render(call: CallbackQuery):
    session = get_session(call.from_user.id)
    if not session["video_msg_id"]:
        return await call.answer("⚠️ ابتدا یک ویدیو ارسال کنید!", show_alert=True)

    await call.answer("🚀 در حال ارسال به سرور پردازش...")
    status_msg = await call.message.edit_text(
        "⚙️ <b>درخواست شما در صف پردازش ابری قرار گرفت!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "🔄 <b>وضعیت:</b> در حال راه‌اندازی سرور پردازشگر...\n"
        "⏳ <i>این پیام به صورت زنده وضعیت پیشرفت را نمایش می‌دهد.</i>"
    )

    success = await trigger_github_workflow(
        chat_id=call.message.chat.id,
        status_msg_id=status_msg.message_id,
        video_msg_id=session["video_msg_id"],
        thumb_msg_id=session["thumb_msg_id"] or 0,
        user_tag=session["tag"]
    )

    if not success:
        await status_msg.edit_text(
            "❌ <b>خطا در ارتباط با سرور پردازش (GitHub Actions)!</b>\n"
            "لطفاً تنظیمات توکن گیت‌هاب را بررسی کنید."
        )
    else:
        # Reset video & thumb for next job while keeping their custom tag
        session["video_msg_id"] = None
        session["thumb_msg_id"] = None

async def trigger_github_workflow(chat_id: int, status_msg_id: int, video_msg_id: int, thumb_msg_id: int, user_tag: str) -> bool:
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
    async with aiohttp.ClientSession() as http_session:
        async with http_session.post(url, json=payload, headers=headers) as resp:
            return resp.status == 204

# ================= RENDER KEEP-ALIVE WEB SERVER =================

async def health_check(request):
    return web.Response(text="Bot is alive and running!")

async def main():
    app = web.Application()
    app.router.add_get("/", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"Keep-alive server started on port {PORT}")

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
