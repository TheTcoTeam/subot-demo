import os
import json
import html
import asyncio
import aiohttp
from datetime import datetime
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.filters import CommandStart, Command
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

# ================= ENVIRONMENT VARIABLES & CONSTANTS =================
BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
GITHUB_PAT = (os.getenv("GITHUB_PAT") or "").strip()
GH_OWNER = (os.getenv("GH_OWNER") or "").strip()
GH_REPO = (os.getenv("GH_REPO") or "").strip()
DEFAULT_TAG = (os.getenv("DEFAULT_TAG") or "@YourChannelTag").strip()
PORT = int(os.getenv("PORT", 10000))
RENDER_URL = os.getenv("RENDER_EXTERNAL_URL") or f"https://{GH_REPO}.onrender.com"

# Exclusive Owner Configuration (@TahaTheGoat)
OWNER_ID = 6261812670
OWNER_USERNAME = "@TahaTheGoat"
ACCESS_DB_FILE = "access_control.json"

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is missing!")

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

http_client: aiohttp.ClientSession | None = None
user_sessions: dict[int, dict] = {}

# ================= STYLE & STUDIO PRESETS =================
COLOR_ORDER = ["yellow", "white", "cyan", "green"]
COLOR_LABELS = {
    "yellow": "🟡 زرد سینمایی",
    "white": "⚪ سفید نئونی",
    "cyan": "🩵 فیروزه‌ای مدرن",
    "green": "🟢 سبز فسفری",
}

SIZE_ORDER = ["normal", "large", "small"]
SIZE_LABELS = {
    "normal": "استاندارد (Normal)",
    "large": "بزرگ (Large)",
    "small": "ظریف (Compact)",
}

TAG_POS_ORDER = ["bottom", "top_right", "top_left", "none"]
TAG_POS_LABELS = {
    "bottom": "پایین وسط",
    "top_right": "بالا راست",
    "top_left": "بالا چپ",
    "none": "بدون تگ (مخفی)",
}

TONE_ORDER = ["uncensored", "cinema"]
TONE_LABELS = {
    "uncensored": "🔥 بزرگسال / بدون سانسور (محاوره‌ای)",
    "cinema": "🎬 سینمایی عمومی (طبیعی)",
}


# ================= ACCESS CONTROL DATABASE =================
def load_access_db() -> dict:
    default_db = {
        "public_mode": False,
        "approved_users": [OWNER_ID],
        "pending_requests": {},
        "users_info": {},
        "total_renders": 0,
    }
    if os.path.exists(ACCESS_DB_FILE):
        try:
            with open(ACCESS_DB_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if OWNER_ID not in data.get("approved_users", []):
                    data.setdefault("approved_users", []).append(OWNER_ID)
                for k, v in default_db.items():
                    data.setdefault(k, v)
                return data
        except Exception:
            pass
    return default_db


def save_access_db(db: dict):
    try:
        with open(ACCESS_DB_FILE, "w", encoding="utf-8") as f:
            json.dump(db, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[AccessDB Save Warning] {e}", flush=True)


access_db = load_access_db()


def register_user_metadata(user):
    if not user:
        return
    uid_str = str(user.id)
    full_name = (user.full_name or "کاربر ناشناس").strip()
    username = f"@{user.username}" if user.username else "ندارد"
    if uid_str not in access_db["users_info"]:
        access_db["users_info"][uid_str] = {
            "user_id": user.id,
            "full_name": full_name,
            "username": username,
            "joined_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        }
        save_access_db(access_db)
    else:
        access_db["users_info"][uid_str]["full_name"] = full_name
        access_db["users_info"][uid_str]["username"] = username


def is_user_authorized(user_id: int) -> bool:
    if user_id == OWNER_ID:
        return True
    if access_db.get("public_mode", False):
        return True
    return user_id in access_db.get("approved_users", [])


# ================= SESSION MANAGEMENT =================
def get_session(user_id: int) -> dict:
    if user_id not in user_sessions:
        user_sessions[user_id] = {
            "video_msg_id": None,
            "video_file_name": "ویدیوی ارسالی",
            "video_size_mb": 0.0,
            "thumb_msg_id": None,
            "panel_msg_id": None,
            "tag": DEFAULT_TAG,
            "sub_color": "yellow",
            "sub_size": "normal",
            "tag_pos": "bottom",
            "tone": "uncensored",
            "awaiting_input": None,
            "is_dispatching": False,
        }
    return user_sessions[user_id]


# ================= UI BUILDERS =================
def build_restricted_text(user) -> str:
    safe_name = html.escape(user.full_name or "کاربر گرامی")
    uid = user.id
    is_pending = str(uid) in access_db.get("pending_requests", {})
    status_badge = "⏳ در انتظار تایید مدیریت" if is_pending else "⛔ غیرفعال (نیازمند مجوز دسترسی)"

    return (
        "🔒 <b>سیستم هوشمند زیرنویس و ترجمه ویدیو (VIP Demo)</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>کاربر:</b> <code>{safe_name}</code>\n"
        f"🆔 <b>شناسه عددی:</b> <code>{uid}</code>\n"
        f"📡 <b>وضعیت دسترسی:</b> {status_badge}\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "⚠️ <b>این ربات در حال حاضر در حالت خصوصی قرار دارد و برای حساب شما غیرفعال است.</b>\n\n"
        "جهت استفاده از موتور پردازش ابری، ابتدا از طریق دکمه زیر <b>درخواست دسترسی</b> خود را ارسال نمایید تا توسط مدیریت بررسی و فعال شود:"
    )


def build_restricted_keyboard(user_id: int) -> InlineKeyboardMarkup:
    is_pending = str(user_id) in access_db.get("pending_requests", {})
    req_btn_text = "⏳ درخواست شما ارسال شده است (در انتظار تایید)" if is_pending else "🚀 ارسال درخواست دسترسی (Request Access)"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=req_btn_text, callback_data="req_access")],
        [
            InlineKeyboardButton(text="🔄 بررسی وضعیت حساب", callback_data="check_access"),
            InlineKeyboardButton(text=f"👑 مدیریت ({OWNER_USERNAME})", url="https://t.me/TahaTheGoat")
        ]
    ])


def build_welcome_keyboard(user_id: int) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="⚙️ پیش‌تنظیمات استودیو", callback_data="open_studio_prefs"),
            InlineKeyboardButton(text="📖 راهنمای امکانات", callback_data="show_help")
        ]
    ]
    if user_id == OWNER_ID:
        pending_count = len(access_db.get("pending_requests", {}))
        badge = f" ({pending_count} 🆕)" if pending_count > 0 else ""
        rows.append([
            InlineKeyboardButton(text=f"👑 پنل مدیریت مالک (Admin Panel){badge}", callback_data="admin_home")
        ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_panel_keyboard(session: dict, user_id: int) -> InlineKeyboardMarkup:
    has_thumb = "✅ تنظیم شد" if session["thumb_msg_id"] else "➕ افزودن"
    color_lbl = COLOR_LABELS.get(session["sub_color"], COLOR_LABELS["yellow"])
    size_lbl = SIZE_LABELS.get(session["sub_size"], SIZE_LABELS["normal"])
    pos_lbl = TAG_POS_LABELS.get(session["tag_pos"], TAG_POS_LABELS["bottom"])
    tone_lbl = TONE_LABELS.get(session["tone"], TONE_LABELS["uncensored"])

    rows = [
        [
            InlineKeyboardButton(text=f"🖼 تامنیل: {has_thumb}", callback_data="set_thumb"),
            InlineKeyboardButton(text=f"🏷 تگ: {session['tag']}", callback_data="set_tag")
        ],
        [
            InlineKeyboardButton(text=f"🎨 رنگ: {color_lbl}", callback_data="cycle_color"),
            InlineKeyboardButton(text=f"📐 سایز: {size_lbl}", callback_data="cycle_size")
        ],
        [
            InlineKeyboardButton(text=f"📍 مکان تگ: {pos_lbl}", callback_data="cycle_tag_pos"),
        ],
        [
            InlineKeyboardButton(text=f"🎭 لحن ترجمه: {tone_lbl}", callback_data="cycle_tone")
        ],
        [
            InlineKeyboardButton(text="🎬 شروع ساخت زیرنویس هوشمند (توربو)", callback_data="start_render")
        ],
        [
            InlineKeyboardButton(text="🔄 بازنشانی پیش‌فرض", callback_data="reset_prefs"),
            InlineKeyboardButton(text="❌ لغو عملیات", callback_data="cancel_job")
        ]
    ]
    if user_id == OWNER_ID:
        rows.append([InlineKeyboardButton(text="👑 پنل مدیریت مالک", callback_data="admin_home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_panel_text(session: dict, file_name: str | None = None) -> str:
    display_name = file_name or session.get("video_file_name") or "ویدیوی ارسالی"
    safe_name = html.escape(display_name)
    safe_tag = html.escape(session["tag"])
    size_mb = session.get("video_size_mb", 0.0)
    size_str = f" (<code>{size_mb:.1f} MB</code>)" if size_mb > 0 else ""
    thumb_status = "✅ کاور اختصاصی ثبت شد" if session["thumb_msg_id"] else "➖ استخراج خودکار از فریم ویدیو"

    color_lbl = COLOR_LABELS.get(session["sub_color"], COLOR_LABELS["yellow"])
    size_lbl = SIZE_LABELS.get(session["sub_size"], SIZE_LABELS["normal"])
    pos_lbl = TAG_POS_LABELS.get(session["tag_pos"], TAG_POS_LABELS["bottom"])
    tone_lbl = TONE_LABELS.get(session["tone"], TONE_LABELS["uncensored"])

    return (
        "💎 <b>استودیو هوشمند زیرنویس و ادیت ویدیو (نسخه توربو پرو)</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"📹 <b>فایل ویدیو:</b> <code>{safe_name}</code>{size_str}\n"
        f"🖼 <b>تامنیل (کاور):</b> {thumb_status}\n"
        f"🏷 <b>تگ واترمارک:</b> <code>{safe_tag}</code> ({pos_lbl})\n"
        f"🎨 <b>رنگ و فونت زیرنویس:</b> {color_lbl} | فونت وزیرمتن\n"
        f"📐 <b>اندازه زیرنویس:</b> {size_lbl}\n"
        f"🎭 <b>موتور ترجمه:</b> {tone_lbl}\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "👇 می‌توانید گزینه‌ها را با یک کلیک شخصی‌سازی کنید یا دکمه <b>شروع</b> را بزنید:"
    )


def build_admin_text() -> str:
    total_users = len(access_db.get("users_info", {}))
    approved_count = len(access_db.get("approved_users", []))
    pending_count = len(access_db.get("pending_requests", {}))
    total_renders = access_db.get("total_renders", 0)
    mode_str = "🔓 عمومی (آزاد برای همه)" if access_db.get("public_mode") else "🔒 خصوصی (فقط کاربران تایید شده)"

    return (
        "👑 <b>پنل مدیریت اختصاصی مالک (@TahaTheGoat)</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 <b>وضعیت دسترسی ربات:</b> <b>{mode_str}</b>\n"
        f"👥 <b>کل کاربران استارت‌زده:</b> <code>{total_users}</code> نفر\n"
        f"✅ <b>کاربران دارای مجوز (VIP):</b> <code>{approved_count}</code> نفر\n"
        f"⏳ <b>درخواست‌های در انتظار تایید:</b> <code>{pending_count}</code> مورد\n"
        f"🎬 <b>کل ویدیوهای پردازش‌شده:</b> <code>{total_renders}</code> ویدیو\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "👇 از منوی زیر بخش مورد نظر خود را انتخاب کنید:"
    )


def build_admin_keyboard() -> InlineKeyboardMarkup:
    pending_count = len(access_db.get("pending_requests", {}))
    approved_count = len(access_db.get("approved_users", []))
    mode_toggle_text = "🔒 قفل کردن ربات (حالت خصوصی)" if access_db.get("public_mode") else "🔓 باز کردن ربات برای همه (عمومی)"

    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=f"📬 درخواست‌های دسترسی ({pending_count})", callback_data="adm_pending_list"),
            InlineKeyboardButton(text=f"👥 کاربران تایید شده ({approved_count})", callback_data="adm_approved_list")
        ],
        [
            InlineKeyboardButton(text="➕ افزودن دستی آیدی عددی", callback_data="adm_add_id"),
            InlineKeyboardButton(text="📢 ارسال پیام همگانی", callback_data="adm_broadcast")
        ],
        [
            InlineKeyboardButton(text=mode_toggle_text, callback_data="adm_toggle_mode")
        ],
        [
            InlineKeyboardButton(text="🔄 بروزرسانی آمار", callback_data="admin_home"),
            InlineKeyboardButton(text="🔙 بازگشت به ربات", callback_data="adm_close")
        ]
    ])


async def refresh_panel_message(message: Message, session: dict, prefix_notice: str = ""):
    old_panel_id = session.get("panel_msg_id")
    if old_panel_id:
        try:
            await bot.delete_message(chat_id=message.chat.id, message_id=old_panel_id)
        except Exception:
            pass

    full_text = f"{prefix_notice}\n\n{build_panel_text(session)}" if prefix_notice else build_panel_text(session)
    sent = await message.reply(
        full_text,
        reply_markup=build_panel_keyboard(session, message.from_user.id)
    )
    session["panel_msg_id"] = sent.message_id


# ================= COMMAND & MESSAGE HANDLERS =================

@dp.message(CommandStart())
async def cmd_start(message: Message):
    register_user_metadata(message.from_user)
    uid = message.from_user.id
    get_session(uid)

    if not is_user_authorized(uid):
        return await message.answer(
            build_restricted_text(message.from_user),
            reply_markup=build_restricted_keyboard(uid)
        )

    welcome_text = (
        "✨ <b>به استودیو هوشمند ترجمه و زیرنویس ویدیو (نسخه توربو پرو) خوش آمدید!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "🚀 <b>قابلیت‌های نسخه حرفه‌ای:</b>\n"
        "• <b>تشخیص فوق‌دقیق گفتار (Whisper Turbo):</b> هماهنگی میلی‌ثانیه‌ای با صدا و قطع خودکار در لحظات سکوت\n"
        "• <b>موتور ترجمه دوگانه:</b> پشتیبانی از دیالوگ‌های صمیمی/بزرگسال (کاملاً محاوره‌ای و بدون سانسور) و سینمایی\n"
        "• <b>شخصی‌سازی کامل استودیویی:</b> انتخاب رنگ زیرنویس، سایز فونت، مکان تگ و کاور اختصاصی\n"
        "• <b>کیفیت خروجی 1080p Full HD:</b> دانلود و آپلود ۶ کاناله برای ویدیوهای حجیم به همراه گزارش زمان پردازش\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📥 <b>برای شروع، کافیست یک ویدیو ارسال یا فوروارد کنید!</b>"
    )
    await message.answer(welcome_text, reply_markup=build_welcome_keyboard(uid))


@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    register_user_metadata(message.from_user)
    if message.from_user.id != OWNER_ID:
        return await message.answer("⛔ <b>شما به پنل مدیریت دسترسی ندارید.</b>")
    session = get_session(OWNER_ID)
    session["awaiting_input"] = None
    await message.answer(build_admin_text(), reply_markup=build_admin_keyboard())


@dp.message(Command("cancel"))
async def cmd_cancel(message: Message):
    session = get_session(message.from_user.id)
    session["video_msg_id"] = None
    session["video_file_name"] = "ویدیوی ارسالی"
    session["video_size_mb"] = 0.0
    session["thumb_msg_id"] = None
    session["awaiting_input"] = None
    session["is_dispatching"] = False
    await message.answer("❌ عملیات جاری لغو شد. هر زمان آماده بودید ویدیوی جدیدی ارسال کنید.")


@dp.message(F.video | F.document | F.video_note)
async def handle_video(message: Message):
    register_user_metadata(message.from_user)
    uid = message.from_user.id
    if not is_user_authorized(uid):
        return await message.reply(
            build_restricted_text(message.from_user),
            reply_markup=build_restricted_keyboard(uid)
        )

    if message.document and not (message.document.mime_type and message.document.mime_type.startswith("video/")):
        return await message.answer("⚠️ لطفاً یک فایل ویدیویی معتبر ارسال کنید.")

    session = get_session(uid)
    session["video_msg_id"] = message.message_id
    session["awaiting_input"] = None
    session["is_dispatching"] = False

    file_name = "Video.mp4"
    file_size = 0
    if message.video:
        file_name = message.video.file_name or "Video.mp4"
        file_size = message.video.file_size or 0
    elif message.document:
        file_name = message.document.file_name or "DocumentVideo.mp4"
        file_size = message.document.file_size or 0
    elif message.video_note:
        file_name = "VideoNote.mp4"
        file_size = message.video_note.file_size or 0

    session["video_file_name"] = file_name
    session["video_size_mb"] = file_size / (1024 * 1024) if file_size else 0.0
    await refresh_panel_message(message, session)


@dp.message(F.photo)
async def handle_photo(message: Message):
    register_user_metadata(message.from_user)
    uid = message.from_user.id
    if not is_user_authorized(uid):
        return await message.reply(
            build_restricted_text(message.from_user),
            reply_markup=build_restricted_keyboard(uid)
        )

    session = get_session(uid)
    session["thumb_msg_id"] = message.message_id
    session["awaiting_input"] = None

    if session["video_msg_id"]:
        await refresh_panel_message(message, session, prefix_notice="✅ <b>تامنیل اختصاصی شما ثبت شد!</b>")
    else:
        await message.reply("✅ تامنیل ذخیره شد! حالا ویدیوی خود را ارسال یا فوروارد کنید.")


@dp.message(F.text)
async def handle_text(message: Message):
    register_user_metadata(message.from_user)
    uid = message.from_user.id
    session = get_session(uid)

    if uid == OWNER_ID and session.get("awaiting_input") == "admin_add_user":
        raw_id = message.text.strip()
        if not raw_id.isdigit():
            return await message.reply("⚠️ لطفاً فقط یک <b>آیدی عددی معتبر</b> ارسال کنید (یا /cancel بزنید):")
        target_id = int(raw_id)
        session["awaiting_input"] = None
        if target_id not in access_db["approved_users"]:
            access_db["approved_users"].append(target_id)
        access_db["pending_requests"].pop(str(target_id), None)
        save_access_db(access_db)
        try:
            await bot.send_message(
                target_id,
                "🎉 <b>تبریک! دسترسی حساب شما توسط مدیریت (@TahaTheGoat) فعال شد!</b>\n"
                "اکنون می‌توانید ویدیوهای خود را برای ترجمه و زیرنویس ارسال کنید."
            )
        except Exception:
            pass
        return await message.reply(
            f"✅ کاربر <code>{target_id}</code> با موفقیت به لیست VIP اضافه شد!",
            reply_markup=build_admin_keyboard()
        )

    if uid == OWNER_ID and session.get("awaiting_input") == "admin_broadcast":
        bcast_text = message.html_text or html.escape(message.text)
        session["awaiting_input"] = None
        sent_count = 0
        all_uids = set(int(k) for k in access_db.get("users_info", {}).keys() if str(k).isdigit())
        all_uids.update(access_db.get("approved_users", []))
        for target_uid in all_uids:
            try:
                await bot.send_message(
                    target_uid,
                    f"📢 <b>اطلاعیه مدیریت ربات:</b>\n━━━━━━━━━━━━━━━━━━━━\n{bcast_text}"
                )
                sent_count += 1
                await asyncio.sleep(0.05)
            except Exception:
                pass
        return await message.reply(
            f"✅ پیام همگانی با موفقیت برای <code>{sent_count}</code> کاربر ارسال شد.",
            reply_markup=build_admin_keyboard()
        )

    if not is_user_authorized(uid):
        return await message.reply(
            build_restricted_text(message.from_user),
            reply_markup=build_restricted_keyboard(uid)
        )

    if session.get("awaiting_input") == "tag":
        new_tag = message.text.strip().replace("__OPT__", "")[:40]
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
        await message.reply(
            "📥 لطفاً یک <b>ویدیو</b> ارسال یا فوروارد کنید تا پنل استودیو باز شود.",
            reply_markup=build_welcome_keyboard(uid)
        )


# ================= ACCESS REQUEST & ADMIN CALLBACKS =================

@dp.callback_query(F.data == "req_access")
async def cb_request_access(call: CallbackQuery):
    register_user_metadata(call.from_user)
    uid = call.from_user.id

    if is_user_authorized(uid):
        await call.answer("✅ حساب شما در حال حاضر فعال است!", show_alert=True)
        return await call.message.edit_text(
            "✅ <b>حساب شما فعال است!</b> کافیست یک ویدیو ارسال یا فوروارد کنید.",
            reply_markup=build_welcome_keyboard(uid)
        )

    uid_str = str(uid)
    if uid_str in access_db["pending_requests"]:
        return await call.answer(
            "⏳ درخواست شما قبلاً ارسال شده و در صف بررسی مدیریت (@TahaTheGoat) قرار دارد.",
            show_alert=True
        )

    full_name = (call.from_user.full_name or "کاربر").strip()
    username = f"@{call.from_user.username}" if call.from_user.username else "ندارد"
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    access_db["pending_requests"][uid_str] = {
        "user_id": uid,
        "full_name": full_name,
        "username": username,
        "requested_at": now_str,
    }
    save_access_db(access_db)

    await call.answer("✅ درخواست دسترسی شما برای مدیریت ارسال شد!", show_alert=True)
    try:
        await call.message.edit_text(
            build_restricted_text(call.from_user),
            reply_markup=build_restricted_keyboard(uid)
        )
    except Exception:
        pass

    safe_name = html.escape(full_name)
    safe_uname = html.escape(username)
    owner_alert = (
        "🔔 <b>درخواست جدید دسترسی به ربات!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>نام کاربر:</b> <code>{safe_name}</code>\n"
        f"🔗 <b>یوزرنیم:</b> {safe_uname}\n"
        f"🆔 <b>آیدی عددی:</b> <code>{uid}</code>\n"
        f"🕒 <b>زمان درخواست:</b> <code>{now_str}</code>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "👇 آیا دسترسی این کاربر را تایید می‌کنید؟"
    )
    owner_kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ تایید دسترسی (Approve)", callback_data=f"adm_approve:{uid}"),
            InlineKeyboardButton(text="❌ رد درخواست (Reject)", callback_data=f"adm_reject:{uid}")
        ],
        [
            InlineKeyboardButton(text="👑 باز کردن پنل مدیریت", callback_data="admin_home")
        ]
    ])
    try:
        await bot.send_message(OWNER_ID, owner_alert, reply_markup=owner_kb)
    except Exception as e:
        print(f"[Owner Alert Error] {e}", flush=True)


@dp.callback_query(F.data == "check_access")
async def cb_check_access(call: CallbackQuery):
    uid = call.from_user.id
    if is_user_authorized(uid):
        await call.answer("🎉 دسترسی شما تایید شده است!", show_alert=True)
        await call.message.edit_text(
            "🎉 <b>حساب شما فعال است!</b>\n"
            "📥 همین حالا یک ویدیو بفرستید یا فوروارد کنید تا پنل استودیو باز شود.",
            reply_markup=build_welcome_keyboard(uid)
        )
    else:
        await call.answer("⏳ حساب شما هنوز توسط مدیریت تایید نشده است.", show_alert=True)


@dp.callback_query(F.data.startswith("adm_approve:"))
async def cb_adm_approve(call: CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return await call.answer("⛔ فقط مالک ربات دسترسی دارد!", show_alert=True)

    target_uid = int(call.data.split(":", 1)[1])
    if target_uid not in access_db["approved_users"]:
        access_db["approved_users"].append(target_uid)
    req_info = access_db["pending_requests"].pop(str(target_uid), None)
    save_access_db(access_db)

    user_label = req_info["full_name"] if req_info else str(target_uid)
    await call.answer(f"✅ دسترسی {user_label} فعال شد!", show_alert=False)

    try:
        await call.message.edit_text(
            f"✅ <b>دسترسی کاربر <code>{html.escape(str(user_label))}</code> (<code>{target_uid}</code>) با موفقیت تایید شد!</b>",
            reply_markup=build_admin_keyboard()
        )
    except Exception:
        pass

    try:
        await bot.send_message(
            target_uid,
            "🎉 <b>تبریک! درخواست دسترسی شما توسط مدیریت (@TahaTheGoat) تایید شد!</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "✅ حساب شما اکنون به صورت کامل فعال است.\n"
            "📥 <b>برای شروع، کافیست یک ویدیو ارسال یا فوروارد کنید!</b>",
            reply_markup=build_welcome_keyboard(target_uid)
        )
    except Exception:
        pass


@dp.callback_query(F.data.startswith("adm_reject:"))
async def cb_adm_reject(call: CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return await call.answer("⛔ فقط مالک ربات دسترسی دارد!", show_alert=True)

    target_uid = int(call.data.split(":", 1)[1])
    access_db["pending_requests"].pop(str(target_uid), None)
    if target_uid in access_db["approved_users"] and target_uid != OWNER_ID:
        access_db["approved_users"].remove(target_uid)
    save_access_db(access_db)

    await call.answer("❌ درخواست کاربر رد شد.", show_alert=False)
    try:
        await call.message.edit_text(
            f"❌ <b>درخواست کاربر <code>{target_uid}</code> رد شد.</b>",
            reply_markup=build_admin_keyboard()
        )
    except Exception:
        pass

    try:
        await bot.send_message(
            target_uid,
            "❌ <b>متاسفانه درخواست دسترسی شما به نسخه دمو مورد تایید قرار نگرفت.</b>\n"
            f"در صورت نیاز با مدیریت ({OWNER_USERNAME}) در ارتباط باشید."
        )
    except Exception:
        pass


@dp.callback_query(F.data.startswith("adm_revoke:"))
async def cb_adm_revoke(call: CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return await call.answer("⛔ عدم دسترسی!", show_alert=True)

    target_uid = int(call.data.split(":", 1)[1])
    if target_uid == OWNER_ID:
        return await call.answer("⚠️ امکان لغو دسترسی مالک وجود ندارد!", show_alert=True)

    if target_uid in access_db["approved_users"]:
        access_db["approved_users"].remove(target_uid)
        save_access_db(access_db)

    await call.answer(f"🚫 دسترسی کاربر {target_uid} لغو شد.", show_alert=True)
    await cb_adm_approved_list(call)


@dp.callback_query(F.data == "admin_home")
async def cb_admin_home(call: CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return await call.answer("⛔ فقط مالک ربات (@TahaTheGoat) دسترسی دارد!", show_alert=True)
    session = get_session(OWNER_ID)
    session["awaiting_input"] = None
    try:
        await call.message.edit_text(build_admin_text(), reply_markup=build_admin_keyboard())
    except Exception:
        pass
    await call.answer()


@dp.callback_query(F.data == "adm_pending_list")
async def cb_adm_pending_list(call: CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return await call.answer("⛔ عدم دسترسی!", show_alert=True)

    pending = access_db.get("pending_requests", {})
    if not pending:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 بازگشت به پنل مدیریت", callback_data="admin_home")]
        ])
        await call.message.edit_text("✅ <b>هیچ درخواست معلقی در صف انتظار وجود ندارد.</b>", reply_markup=kb)
        return await call.answer()

    text_lines = ["📬 <b>لیست درخواست‌های دسترسی در انتظار تایید:</b>\n━━━━━━━━━━━━━━━━━━━━"]
    kb_rows = []
    for uid_str, info in list(pending.items())[:15]:
        name = html.escape(info.get("full_name", "کاربر"))
        uname = html.escape(info.get("username", "ندارد"))
        text_lines.append(f"• <b>{name}</b> ({uname}) | <code>{uid_str}</code>")
        kb_rows.append([
            InlineKeyboardButton(text=f"✅ تایید {info.get('full_name', uid_str)[:15]}", callback_data=f"adm_approve:{uid_str}"),
            InlineKeyboardButton(text="❌ رد", callback_data=f"adm_reject:{uid_str}")
        ])

    kb_rows.append([InlineKeyboardButton(text="🔙 بازگشت به پنل مدیریت", callback_data="admin_home")])
    await call.message.edit_text("\n".join(text_lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))
    await call.answer()


@dp.callback_query(F.data == "adm_approved_list")
async def cb_adm_approved_list(call: CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return await call.answer("⛔ عدم دسترسی!", show_alert=True)

    approved = access_db.get("approved_users", [])
    users_info = access_db.get("users_info", {})
    text_lines = ["👥 <b>لیست کاربران دارای دسترسی (VIP):</b>\n━━━━━━━━━━━━━━━━━━━━"]
    kb_rows = []

    for uid in approved[:20]:
        uinfo = users_info.get(str(uid), {})
        name = html.escape(uinfo.get("full_name", "کاربر تاییدشده"))
        uname = html.escape(uinfo.get("username", ""))
        owner_tag = " (👑 مالک)" if uid == OWNER_ID else ""
        text_lines.append(f"• <b>{name}</b> {uname} — <code>{uid}</code>{owner_tag}")
        if uid != OWNER_ID:
            kb_rows.append([
                InlineKeyboardButton(
                    text=f"🚫 لغو دسترسی {uinfo.get('full_name', uid)}",
                    callback_data=f"adm_revoke:{uid}"
                )
            ])

    kb_rows.append([InlineKeyboardButton(text="🔙 بازگشت به پنل مدیریت", callback_data="admin_home")])
    await call.message.edit_text("\n".join(text_lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))
    await call.answer()


@dp.callback_query(F.data == "adm_add_id")
async def cb_adm_add_id(call: CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return await call.answer("⛔ عدم دسترسی!", show_alert=True)
    session = get_session(OWNER_ID)
    session["awaiting_input"] = "admin_add_user"
    await call.message.answer(
        "➕ <b>لطفاً آیدی عددی (Numeric ID) کاربر مورد نظر را ارسال کنید:</b>\n"
        "<i>(برای انصراف دستور /cancel را بزنید)</i>"
    )
    await call.answer()


@dp.callback_query(F.data == "adm_broadcast")
async def cb_adm_broadcast(call: CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return await call.answer("⛔ عدم دسترسی!", show_alert=True)
    session = get_session(OWNER_ID)
    session["awaiting_input"] = "admin_broadcast"
    await call.message.answer(
        "📢 <b>متن پیام همگانی خود را ارسال کنید تا برای تمام کاربران ارسال شود:</b>\n"
        "<i>(برای انصراف دستور /cancel را بزنید)</i>"
    )
    await call.answer()


@dp.callback_query(F.data == "adm_toggle_mode")
async def cb_adm_toggle_mode(call: CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return await call.answer("⛔ عدم دسترسی!", show_alert=True)
    access_db["public_mode"] = not access_db.get("public_mode", False)
    save_access_db(access_db)
    mode_notice = "🔓 ربات به حالت عمومی تغییر یافت!" if access_db["public_mode"] else "🔒 ربات به حالت خصوصی (VIP) تغییر یافت!"
    await call.answer(mode_notice, show_alert=True)
    await call.message.edit_text(build_admin_text(), reply_markup=build_admin_keyboard())


@dp.callback_query(F.data == "adm_close")
async def cb_adm_close(call: CallbackQuery):
    uid = call.from_user.id
    await call.message.edit_text(
        "✅ <b>پنل مدیریت بسته شد.</b>\n📥 برای شروع پردازش، یک ویدیو ارسال یا فوروارد کنید:",
        reply_markup=build_welcome_keyboard(uid)
    )
    await call.answer()


# ================= STUDIO CUSTOMIZATION CALLBACKS =================

@dp.callback_query(F.data == "open_studio_prefs")
async def cb_open_studio_prefs(call: CallbackQuery):
    uid = call.from_user.id
    if not is_user_authorized(uid):
        return await call.answer("⛔ حساب شما هنوز فعال نشده است!", show_alert=True)
    session = get_session(uid)
    await call.message.edit_text(
        build_panel_text(session),
        reply_markup=build_panel_keyboard(session, uid)
    )
    await call.answer()


@dp.callback_query(F.data == "show_help")
async def cb_show_help(call: CallbackQuery):
    help_text = (
        "📖 <b>راهنمای استودیو هوشمند زیرنویس (نسخه توربو پرو)</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "1️⃣ <b>ارسال ویدیو:</b> هر ویدیویی (حتی با حجم بالا تا ۲ گیگابایت) را به ربات بفرستید یا فوروارد کنید.\n"
        "2️⃣ <b>شخصی‌سازی استودیو:</b>\n"
        "   • <b>🎨 رنگ زیرنویس:</b> زرد سینمایی، سفید نئونی، فیروزه‌ای یا سبز.\n"
        "   • <b>📐 سایز فونت:</b> استاندارد، بزرگ یا ظریف (متناسب با ویدیوهای افقی و عمودی).\n"
        "   • <b>📍 مکان تگ:</b> درج آیدی کانال شما در پایین، بالا-راست، بالا-چپ یا مخفی.\n"
        "   • <b>🎭 لحن ترجمه:</b> حالت <i>بزرگسال/بدون سانسور</i> (ترجمه دقیق اصطلاحات صمیمی و اروتیک به فارسی کاملاً محاوره‌ای) یا <i>سینمایی عمومی</i>.\n"
        "3️⃣ <b>شروع پردازش:</b> پس از کلیک روی دکمه شروع، پیشرفت کار به صورت زنده نمایش داده می‌شود و در پایان، ویدیو + گزارش زمان دقیق پردازش برایتان ارسال می‌گردد."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ باز کردن تنظیمات استودیو", callback_data="open_studio_prefs")]
    ])
    await call.message.edit_text(help_text, reply_markup=kb)
    await call.answer()


@dp.callback_query(F.data == "cycle_color")
async def cb_cycle_color(call: CallbackQuery):
    uid = call.from_user.id
    if not is_user_authorized(uid):
        return await call.answer("⛔ عدم دسترسی!", show_alert=True)
    session = get_session(uid)
    cur_idx = COLOR_ORDER.index(session["sub_color"]) if session["sub_color"] in COLOR_ORDER else 0
    session["sub_color"] = COLOR_ORDER[(cur_idx + 1) % len(COLOR_ORDER)]
    await call.message.edit_text(build_panel_text(session), reply_markup=build_panel_keyboard(session, uid))
    await call.answer(f"🎨 رنگ زیرنویس: {COLOR_LABELS[session['sub_color']]}")


@dp.callback_query(F.data == "cycle_size")
async def cb_cycle_size(call: CallbackQuery):
    uid = call.from_user.id
    if not is_user_authorized(uid):
        return await call.answer("⛔ عدم دسترسی!", show_alert=True)
    session = get_session(uid)
    cur_idx = SIZE_ORDER.index(session["sub_size"]) if session["sub_size"] in SIZE_ORDER else 0
    session["sub_size"] = SIZE_ORDER[(cur_idx + 1) % len(SIZE_ORDER)]
    await call.message.edit_text(build_panel_text(session), reply_markup=build_panel_keyboard(session, uid))
    await call.answer(f"📐 سایز فونت: {SIZE_LABELS[session['sub_size']]}")


@dp.callback_query(F.data == "cycle_tag_pos")
async def cb_cycle_tag_pos(call: CallbackQuery):
    uid = call.from_user.id
    if not is_user_authorized(uid):
        return await call.answer("⛔ عدم دسترسی!", show_alert=True)
    session = get_session(uid)
    cur_idx = TAG_POS_ORDER.index(session["tag_pos"]) if session["tag_pos"] in TAG_POS_ORDER else 0
    session["tag_pos"] = TAG_POS_ORDER[(cur_idx + 1) % len(TAG_POS_ORDER)]
    await call.message.edit_text(build_panel_text(session), reply_markup=build_panel_keyboard(session, uid))
    await call.answer(f"📍 مکان تگ: {TAG_POS_LABELS[session['tag_pos']]}")


@dp.callback_query(F.data == "cycle_tone")
async def cb_cycle_tone(call: CallbackQuery):
    uid = call.from_user.id
    if not is_user_authorized(uid):
        return await call.answer("⛔ عدم دسترسی!", show_alert=True)
    session = get_session(uid)
    cur_idx = TONE_ORDER.index(session["tone"]) if session["tone"] in TONE_ORDER else 0
    session["tone"] = TONE_ORDER[(cur_idx + 1) % len(TONE_ORDER)]
    await call.message.edit_text(build_panel_text(session), reply_markup=build_panel_keyboard(session, uid))
    await call.answer(f"🎭 لحن ترجمه: {TONE_LABELS[session['tone']]}")


@dp.callback_query(F.data == "reset_prefs")
async def cb_reset_prefs(call: CallbackQuery):
    uid = call.from_user.id
    if not is_user_authorized(uid):
        return await call.answer("⛔ عدم دسترسی!", show_alert=True)
    session = get_session(uid)
    session["tag"] = DEFAULT_TAG
    session["thumb_msg_id"] = None
    session["sub_color"] = "yellow"
    session["sub_size"] = "normal"
    session["tag_pos"] = "bottom"
    session["tone"] = "uncensored"
    await call.message.edit_text(build_panel_text(session), reply_markup=build_panel_keyboard(session, uid))
    await call.answer("🔄 تنظیمات به حالت پیش‌فرض بازنشانی شد.")


@dp.callback_query(F.data == "set_thumb")
async def cb_set_thumb(call: CallbackQuery):
    uid = call.from_user.id
    if not is_user_authorized(uid):
        return await call.answer("⛔ عدم دسترسی!", show_alert=True)
    session = get_session(uid)
    session["awaiting_input"] = "thumb"
    await call.message.answer("🖼 لطفاً عکس <b>تامنیل (کاور)</b> مورد نظر خود را همینجا ارسال کنید:")
    await call.answer()


@dp.callback_query(F.data == "set_tag")
async def cb_set_tag(call: CallbackQuery):
    uid = call.from_user.id
    if not is_user_authorized(uid):
        return await call.answer("⛔ عدم دسترسی!", show_alert=True)
    session = get_session(uid)
    session["awaiting_input"] = "tag"
    await call.message.answer("🏷 لطفاً متن <b>تگ واترمارک</b> را ارسال کنید (مثال: <code>@MyChannel</code>):")
    await call.answer()


@dp.callback_query(F.data == "cancel_job")
async def cb_cancel_job(call: CallbackQuery):
    session = get_session(call.from_user.id)
    session["video_msg_id"] = None
    session["video_file_name"] = "ویدیوی ارسالی"
    session["video_size_mb"] = 0.0
    session["thumb_msg_id"] = None
    session["panel_msg_id"] = None
    session["awaiting_input"] = None
    session["is_dispatching"] = False
    await call.message.edit_text("❌ عملیات لغو شد. هر زمان آماده بودید ویدیوی جدیدی ارسال کنید.")
    await call.answer()


@dp.callback_query(F.data == "start_render")
async def cb_start_render(call: CallbackQuery):
    uid = call.from_user.id
    if not is_user_authorized(uid):
        return await call.answer("⛔ حساب شما مجوز دسترسی ندارد!", show_alert=True)

    session = get_session(uid)
    if session.get("is_dispatching"):
        return await call.answer("⏳ درخواست شما در حال ارسال است، لطفاً صبر کنید...", show_alert=False)

    if not session["video_msg_id"]:
        return await call.answer("⚠️ ابتدا یک ویدیو ارسال یا فوروارد کنید!", show_alert=True)

    session["is_dispatching"] = True
    video_msg_id = session["video_msg_id"]
    thumb_msg_id = session["thumb_msg_id"] or 0

    # Pack studio options into user_tag so GitHub's 5-input workflow never throws HTTP 422!
    clean_tag = session["tag"].replace("__OPT__", "").strip()
    opt_suffix = f"{session.get('sub_color', 'yellow')},{session.get('sub_size', 'normal')},{session.get('tag_pos', 'bottom')},{session.get('tone', 'uncensored')}"
    packed_user_tag = f"{clean_tag}__OPT__{opt_suffix}"

    await call.answer("🚀 در حال ارسال به سرور پردازش توربو...")
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
        user_tag=packed_user_tag
    )

    session["is_dispatching"] = False

    if not success:
        await status_msg.edit_text(
            "❌ <b>خطا در ارتباط با سرور پردازش (GitHub Actions)!</b>\n"
            "لطفاً تنظیمات توکن گیت‌هاب (GITHUB_PAT / GH_OWNER / GH_REPO) را بررسی کنید.",
            reply_markup=build_panel_keyboard(session, uid)
        )
    else:
        access_db["total_renders"] = access_db.get("total_renders", 0) + 1
        save_access_db(access_db)
        session["video_msg_id"] = None
        session["video_file_name"] = "ویدیوی ارسالی"
        session["video_size_mb"] = 0.0
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
