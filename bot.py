# ============================================================
# Telegram Subscription Management Bot
# Author: Production-Ready Build
# Features: TxID + Screenshot Proof, Multi-Currency, Auto-Kick
# Stack: pyTelegramBotAPI + MongoDB + APScheduler + Flask
# ============================================================

import os
import time
import logging
from datetime import datetime, timedelta
from threading import Thread

import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from pymongo import MongoClient, errors as mongo_errors
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# RENDER KEEP-ALIVE SERVER
# ─────────────────────────────────────────────
flask_app = Flask("")


@flask_app.route("/")
def home():
    return "✅ Bot is healthy and running!", 200


@flask_app.route("/health")
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}, 200


def run_web():
    port = int(os.environ.get("PORT", 5000))
    flask_app.run(host="0.0.0.0", port=port, use_reloader=False)


# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
BOT_TOKEN        = os.getenv("BOT_TOKEN")
MONGO_URI        = os.getenv("MONGO_URI")
ADMIN_ID         = int(os.getenv("ADMIN_ID", "0"))
CONTACT_USERNAME = os.getenv("CONTACT_USERNAME", "@admin")

if not BOT_TOKEN or not MONGO_URI or ADMIN_ID == 0:
    raise EnvironmentError(
        "Missing required environment variables: BOT_TOKEN, MONGO_URI, ADMIN_ID"
    )

# ─────────────────────────────────────────────
# MONGODB CONNECTION WITH RETRIES
# ─────────────────────────────────────────────
def connect_mongo(uri: str, retries: int = 5, delay: int = 3) -> MongoClient:
    for attempt in range(1, retries + 1):
        try:
            client = MongoClient(
                uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                socketTimeoutMS=10000,
            )
            client.admin.command("ping")
            logger.info("✅ MongoDB connected successfully.")
            return client
        except mongo_errors.ServerSelectionTimeoutError as e:
            logger.warning(f"MongoDB attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(delay)
    raise ConnectionError("❌ Could not connect to MongoDB after multiple retries.")


client       = connect_mongo(MONGO_URI)
db           = client["sub_management"]
channels_col = db["channels"]      # Registered private channels
users_col    = db["users"]         # Active subscriptions
gateways_col = db["gateways"]      # Payment gateways
pending_col  = db["pending"]       # Pending payment proofs

# Ensure indexes for performance
users_col.create_index("expiry")
users_col.create_index([("user_id", 1), ("channel_id", 1)])
pending_col.create_index([("user_id", 1), ("channel_id", 1)])

# ─────────────────────────────────────────────
# BOT INSTANCE
# ─────────────────────────────────────────────
bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)

# ─────────────────────────────────────────────
# HELPER UTILITIES
# ─────────────────────────────────────────────
def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def format_duration(minutes: int) -> str:
    """Convert raw minutes to a human-readable string."""
    if minutes < 60:
        return f"{minutes} Minute{'s' if minutes > 1 else ''}"
    elif minutes < 1440:
        hours = minutes // 60
        return f"{hours} Hour{'s' if hours > 1 else ''}"
    else:
        days = minutes // 1440
        return f"{days} Day{'s' if days > 1 else ''}"


def admin_only_markup() -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("📢 Add Channel",   callback_data="adm_add_ch"),
        InlineKeyboardButton("💰 Add Gateway",   callback_data="adm_add_gw"),
        InlineKeyboardButton("📋 List Channels", callback_data="adm_list_ch"),
        InlineKeyboardButton("🏦 List Gateways", callback_data="adm_list_gw"),
    )
    return markup


# ─────────────────────────────────────────────
# /start COMMAND
# ─────────────────────────────────────────────
@bot.message_handler(commands=["start"])
def start_handler(message):
    user_id = message.from_user.id
    args    = message.text.split()

    # ── DEEP LINK ENTRY (User flow) ──
    if len(args) > 1:
        try:
            ch_id   = int(args[1])
            ch_data = channels_col.find_one({"channel_id": ch_id})
            if ch_data:
                plans   = ch_data.get("plans", {})
                markup  = InlineKeyboardMarkup(row_width=1)
                for mins_str, price in plans.items():
                    label = format_duration(int(mins_str))
                    markup.add(
                        InlineKeyboardButton(
                            f"💳 {label}  —  {price}",
                            callback_data=f"cur_{ch_id}_{mins_str}",
                        )
                    )
                bot.send_message(
                    message.chat.id,
                    f"💎 *Welcome!*\n\nYou are subscribing to:\n*{ch_data['name']}*\n\n"
                    f"Please select a subscription plan below 👇",
                    reply_markup=markup,
                    parse_mode="Markdown",
                )
                return
            else:
                bot.send_message(
                    message.chat.id,
                    "❌ This channel is not registered. Please contact the admin.",
                )
                return
        except (ValueError, TypeError):
            pass

    # ── ADMIN PANEL ──
    if is_admin(user_id):
        bot.send_message(
            message.chat.id,
            "🛠 *Admin Power Panel*\n\nWelcome back, Admin! Choose an action:",
            reply_markup=admin_only_markup(),
            parse_mode="Markdown",
        )
    else:
        bot.send_message(
            message.chat.id,
            f"👋 Hello! Please use the correct subscription link to get started.\n\n"
            f"Need help? Contact {CONTACT_USERNAME}",
        )


# ─────────────────────────────────────────────
# ADMIN: /add — Add Channel via Forward
# ─────────────────────────────────────────────
@bot.message_handler(commands=["add"])
def cmd_add_channel(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(
        ADMIN_ID,
        "📢 *Add New Channel*\n\nForward any message from your *private channel* to register it.\n\n"
        "⚠️ Make sure the bot is already an admin in that channel with *Invite Users* permission.",
        parse_mode="Markdown",
    )
    bot.register_next_step_handler(msg, save_channel)


@bot.callback_query_handler(func=lambda c: c.data == "adm_add_ch")
def add_ch_init(call):
    if not is_admin(call.from_user.id):
        return bot.answer_callback_query(call.id, "❌ Unauthorized")
    bot.answer_callback_query(call.id)
    msg = bot.send_message(
        ADMIN_ID,
        "📢 *Add New Channel*\n\nForward any message from your *private channel* to register it.\n\n"
        "⚠️ Make sure the bot is already an admin in that channel with *Invite Users* permission.",
        parse_mode="Markdown",
    )
    bot.register_next_step_handler(msg, save_channel)


def save_channel(message):
    if not message.forward_from_chat:
        bot.send_message(
            ADMIN_ID,
            "❌ That wasn't a forwarded message from a channel. Please try again with /add",
        )
        return
    ch_id   = message.forward_from_chat.id
    ch_name = message.forward_from_chat.title
    msg = bot.send_message(
        ADMIN_ID,
        f"✅ Channel detected: *{ch_name}*\n\n"
        f"Now send the subscription plans in this format:\n"
        f"`Minutes:Price, Minutes:Price`\n\n"
        f"📌 Example:\n`1440:100 BDT, 43200:500 BDT, 525600:2000 BDT`",
        parse_mode="Markdown",
    )
    bot.register_next_step_handler(msg, finalize_channel, ch_id, ch_name)


def finalize_channel(message, ch_id, ch_name):
    try:
        raw_plans = message.text.strip().split(",")
        plans = {}
        for entry in raw_plans:
            mins, price = entry.strip().split(":")
            plans[mins.strip()] = price.strip()
        if not plans:
            raise ValueError("No plans parsed")
    except Exception:
        bot.send_message(
            ADMIN_ID,
            "❌ Invalid format. Please use: `1440:100 BDT, 43200:500 BDT`",
            parse_mode="Markdown",
        )
        return

    channels_col.update_one(
        {"channel_id": ch_id},
        {"$set": {"channel_id": ch_id, "name": ch_name, "plans": plans, "added_at": datetime.utcnow()}},
        upsert=True,
    )
    bot_username = bot.get_me().username
    deep_link    = f"https://t.me/{bot_username}?start={ch_id}"
    bot.send_message(
        ADMIN_ID,
        f"✅ *Channel Registered Successfully!*\n\n"
        f"📢 Channel: *{ch_name}*\n"
        f"🔗 Share this link with users:\n`{deep_link}`\n\n"
        f"📋 Plans saved: *{len(plans)}*",
        parse_mode="Markdown",
    )


# ─────────────────────────────────────────────
# ADMIN: /gateway — Add Payment Gateway
# ─────────────────────────────────────────────
@bot.message_handler(commands=["gateway"])
def cmd_add_gateway(message):
    if not is_admin(message.from_user.id):
        return
    _prompt_gateway(message.chat.id)


@bot.callback_query_handler(func=lambda c: c.data == "adm_add_gw")
def add_gateway_init(call):
    if not is_admin(call.from_user.id):
        return bot.answer_callback_query(call.id, "❌ Unauthorized")
    bot.answer_callback_query(call.id)
    _prompt_gateway(call.message.chat.id)


def _prompt_gateway(chat_id):
    msg = bot.send_message(
        chat_id,
        "🏦 *Add Payment Gateway*\n\n"
        "Send gateway details in this format:\n"
        "`Currency,MethodName,Details,Instructions`\n\n"
        "📌 Examples:\n"
        "`BDT,bKash,01712345678,Send Money — use your mobile number`\n"
        "`INR,UPI,merchant@ybl,Pay via QR or UPI ID`\n"
        "`USD,PayPal,pay@example.com,Friends & Family only`\n"
        "`USDT,Binance,0xABC123...,TRC20 network only`",
        parse_mode="Markdown",
    )
    bot.register_next_step_handler(msg, save_gateway)


def save_gateway(message):
    try:
        parts = [x.strip() for x in message.text.split(",", 3)]
        if len(parts) != 4:
            raise ValueError("Need exactly 4 comma-separated parts")
        curr, method, details, instructions = parts
        gateways_col.update_one(
            {"method_name": method},
            {
                "$set": {
                    "currency":     curr.upper(),
                    "method_name":  method,
                    "details":      details,
                    "instructions": instructions,
                    "updated_at":   datetime.utcnow(),
                }
            },
            upsert=True,
        )
        bot.send_message(
            ADMIN_ID,
            f"✅ *Gateway Added/Updated!*\n\n"
            f"💱 Currency: `{curr.upper()}`\n"
            f"🏦 Method:   `{method}`\n"
            f"📋 Details:  `{details}`",
            parse_mode="Markdown",
        )
    except Exception as e:
        bot.send_message(
            ADMIN_ID,
            f"❌ Format error: `{e}`\n\nUse: `Currency,Method,Details,Instructions`",
            parse_mode="Markdown",
        )


# ─────────────────────────────────────────────
# ADMIN: /channels — List Registered Channels
# ─────────────────────────────────────────────
@bot.message_handler(commands=["channels"])
def cmd_list_channels(message):
    if not is_admin(message.from_user.id):
        return
    _list_channels(message.chat.id)


@bot.callback_query_handler(func=lambda c: c.data == "adm_list_ch")
def list_channels_cb(call):
    if not is_admin(call.from_user.id):
        return bot.answer_callback_query(call.id, "❌ Unauthorized")
    bot.answer_callback_query(call.id)
    _list_channels(call.message.chat.id)


def _list_channels(chat_id):
    channels = list(channels_col.find())
    if not channels:
        bot.send_message(chat_id, "📭 No channels registered yet. Use /add to add one.")
        return
    bot_username = bot.get_me().username
    lines = ["📋 *Registered Channels*\n"]
    for i, ch in enumerate(channels, 1):
        link  = f"https://t.me/{bot_username}?start={ch['channel_id']}"
        plans = ", ".join(
            f"{format_duration(int(m))} → {p}" for m, p in ch.get("plans", {}).items()
        )
        lines.append(
            f"*{i}. {ch['name']}*\n"
            f"   🔗 `{link}`\n"
            f"   📦 Plans: {plans}\n"
        )
    bot.send_message(chat_id, "\n".join(lines), parse_mode="Markdown")


# ─────────────────────────────────────────────
# ADMIN: /gateways — List Payment Gateways
# ─────────────────────────────────────────────
@bot.message_handler(commands=["gateways"])
def cmd_list_gateways(message):
    if not is_admin(message.from_user.id):
        return
    _list_gateways(message.chat.id)


@bot.callback_query_handler(func=lambda c: c.data == "adm_list_gw")
def list_gateways_cb(call):
    if not is_admin(call.from_user.id):
        return bot.answer_callback_query(call.id, "❌ Unauthorized")
    bot.answer_callback_query(call.id)
    _list_gateways(call.message.chat.id)


def _list_gateways(chat_id):
    gateways = list(gateways_col.find())
    if not gateways:
        bot.send_message(chat_id, "📭 No gateways registered yet. Use /gateway to add one.")
        return
    lines = ["🏦 *Registered Payment Gateways*\n"]
    for i, gw in enumerate(gateways, 1):
        lines.append(
            f"*{i}. {gw['method_name']}*\n"
            f"   💱 Currency:     `{gw['currency']}`\n"
            f"   📋 Details:      `{gw['details']}`\n"
            f"   📝 Instructions: {gw['instructions']}\n"
        )
    bot.send_message(chat_id, "\n".join(lines), parse_mode="Markdown")


# ─────────────────────────────────────────────
# USER FLOW — STEP 1: SELECT CURRENCY
# ─────────────────────────────────────────────
@bot.callback_query_handler(func=lambda c: c.data.startswith("cur_"))
def show_currencies(call):
    try:
        _, ch_id, mins = call.data.split("_", 2)
    except ValueError:
        return
    currencies = gateways_col.distinct("currency")
    if not currencies:
        bot.answer_callback_query(call.id, "❌ No payment gateways configured yet.")
        return bot.send_message(
            call.message.chat.id,
            f"❌ Admin hasn't set up any payment gateways yet.\nContact: {CONTACT_USERNAME}",
        )
    markup = InlineKeyboardMarkup(row_width=2)
    currency_flags = {"BDT": "🇧🇩", "INR": "🇮🇳", "USD": "🇺🇸", "USDT": "💵"}
    for cur in sorted(currencies):
        flag = currency_flags.get(cur, "💱")
        markup.add(
            InlineKeyboardButton(
                f"{flag} Pay in {cur}",
                callback_data=f"paymeth_{ch_id}_{mins}_{cur}",
            )
        )
    bot.edit_message_text(
        "🌍 *Select Your Currency*\n\nChoose the currency you want to pay in:",
        call.message.chat.id,
        call.message.message_id,
        reply_markup=markup,
        parse_mode="Markdown",
    )
    bot.answer_callback_query(call.id)


# ─────────────────────────────────────────────
# USER FLOW — STEP 2: SELECT PAYMENT METHOD
# ─────────────────────────────────────────────
@bot.callback_query_handler(func=lambda c: c.data.startswith("paymeth_"))
def show_methods(call):
    try:
        parts  = call.data.split("_", 3)
        ch_id  = parts[1]
        mins   = parts[2]
        curr   = parts[3]
    except (ValueError, IndexError):
        return
    methods = list(gateways_col.find({"currency": curr}))
    if not methods:
        bot.answer_callback_query(call.id, "❌ No methods for this currency.")
        return
    markup = InlineKeyboardMarkup(row_width=1)
    for m in methods:
        markup.add(
            InlineKeyboardButton(
                f"💳 {m['method_name']}",
                callback_data=f"final_{ch_id}_{mins}_{m['method_name']}",
            )
        )
    bot.edit_message_text(
        f"💳 *Select {curr} Payment Method*\n\nChoose your preferred payment option:",
        call.message.chat.id,
        call.message.message_id,
        reply_markup=markup,
        parse_mode="Markdown",
    )
    bot.answer_callback_query(call.id)


# ─────────────────────────────────────────────
# USER FLOW — STEP 3: SHOW PAYMENT DETAILS
# ─────────────────────────────────────────────
@bot.callback_query_handler(func=lambda c: c.data.startswith("final_"))
def final_pay_step(call):
    try:
        parts = call.data.split("_", 3)
        ch_id = int(parts[1])
        mins  = parts[2]
        meth  = parts[3]
    except (ValueError, IndexError):
        return
    gw      = gateways_col.find_one({"method_name": meth})
    ch_data = channels_col.find_one({"channel_id": ch_id})
    if not gw or not ch_data:
        bot.answer_callback_query(call.id, "❌ Data not found.")
        return
    price    = ch_data["plans"].get(mins, "N/A")
    duration = format_duration(int(mins))
    text = (
        f"📦 *Subscription Details*\n"
        f"{'─' * 30}\n"
        f"📺 Channel:  *{ch_data['name']}*\n"
        f"⏱ Duration: *{duration}*\n"
        f"💰 Price:    *{price}*\n\n"
        f"🏦 *Payment Details*\n"
        f"{'─' * 30}\n"
        f"💳 Method:   *{meth}*\n"
        f"📋 Account:  `{gw['details']}`\n"
        f"📝 Note:     {gw['instructions']}\n\n"
        f"⚡ After paying, click *'I Have Paid'* below."
    )
    markup = InlineKeyboardMarkup()
    markup.add(
        InlineKeyboardButton("✅ I Have Paid", callback_data=f"req_{ch_id}_{mins}_{meth}")
    )
    bot.edit_message_text(
        text,
        call.message.chat.id,
        call.message.message_id,
        reply_markup=markup,
        parse_mode="Markdown",
    )
    bot.answer_callback_query(call.id)


# ─────────────────────────────────────────────
# USER FLOW — STEP 4: REQUEST TXID (New Feature)
# ─────────────────────────────────────────────
@bot.callback_query_handler(func=lambda c: c.data.startswith("req_"))
def request_txid(call):
    try:
        parts = call.data.split("_", 3)
        ch_id = parts[1]
        mins  = parts[2]
        meth  = parts[3]
    except (ValueError, IndexError):
        return

    bot.answer_callback_query(call.id, "✅ Please provide your Transaction ID")

    # Store pending context in DB so it survives restarts
    pending_col.update_one(
        {"user_id": call.from_user.id},
        {
            "$set": {
                "user_id":    call.from_user.id,
                "username":   call.from_user.username or "",
                "first_name": call.from_user.first_name or "",
                "channel_id": int(ch_id),
                "mins":       mins,
                "method":     meth,
                "step":       "awaiting_txid",
                "created_at": datetime.utcnow(),
            }
        },
        upsert=True,
    )

    msg = bot.send_message(
        call.from_user.id,
        "🔐 *Payment Proof — Step 1 of 2*\n\n"
        "Please send your *Transaction ID (TxID)* or *Reference Number* exactly as shown by the payment app.\n\n"
        "📌 Example: `TXN123456789` or `BK2024ABC`",
        parse_mode="Markdown",
    )
    bot.register_next_step_handler(msg, receive_txid)


# ─────────────────────────────────────────────
# USER FLOW — STEP 5: RECEIVE TXID
# ─────────────────────────────────────────────
def receive_txid(message):
    user_id = message.from_user.id
    txid    = message.text.strip() if message.text else None

    if not txid:
        msg = bot.send_message(
            user_id,
            "❌ Invalid input. Please send the *Transaction ID* as plain text.",
            parse_mode="Markdown",
        )
        bot.register_next_step_handler(msg, receive_txid)
        return

    # Update pending record with txid
    pending_col.update_one(
        {"user_id": user_id},
        {"$set": {"txid": txid, "step": "awaiting_screenshot"}},
    )

    msg = bot.send_message(
        user_id,
        f"✅ TxID received: `{txid}`\n\n"
        f"📸 *Payment Proof — Step 2 of 2*\n\n"
        f"Now send a *screenshot* of your payment confirmation as a *photo* (not as a file/document).",
        parse_mode="Markdown",
    )
    bot.register_next_step_handler(msg, receive_screenshot)


# ─────────────────────────────────────────────
# USER FLOW — STEP 6: RECEIVE SCREENSHOT
# ─────────────────────────────────────────────
def receive_screenshot(message):
    user_id = message.from_user.id

    if not message.photo:
        msg = bot.send_message(
            user_id,
            "❌ Please send the screenshot as a *photo*, not as a file.\n\n"
            "Tap the 📎 attachment icon and choose *Photo*.",
            parse_mode="Markdown",
        )
        bot.register_next_step_handler(msg, receive_screenshot)
        return

    photo_file_id = message.photo[-1].file_id  # Highest quality

    # Fetch pending record
    pending = pending_col.find_one({"user_id": user_id})
    if not pending:
        bot.send_message(user_id, "❌ Session expired. Please start again from the channel link.")
        return

    # Update pending with screenshot and mark complete
    pending_col.update_one(
        {"user_id": user_id},
        {"$set": {"screenshot_file_id": photo_file_id, "step": "submitted"}},
    )

    # Notify the user
    bot.send_message(
        user_id,
        "🎉 *Payment Proof Submitted!*\n\n"
        "✅ Your Transaction ID and screenshot have been received.\n"
        "⏳ Please wait while the admin reviews your payment.\n\n"
        f"Need help? Contact {CONTACT_USERNAME}",
        parse_mode="Markdown",
    )

    # ── NOTIFY ADMIN WITH FULL PROOF ──
    ch_data = channels_col.find_one({"channel_id": pending["channel_id"]})
    ch_name = ch_data["name"] if ch_data else str(pending["channel_id"])
    price   = (ch_data["plans"].get(pending["mins"], "N/A") if ch_data else "N/A")
    duration = format_duration(int(pending["mins"]))

    user_mention = (
        f"@{pending['username']}" if pending.get("username") else pending.get("first_name", "Unknown")
    )

    caption = (
        f"🔔 *New Payment Request*\n"
        f"{'─' * 30}\n"
        f"👤 User:      {user_mention} (`{user_id}`)\n"
        f"📺 Channel:  *{ch_name}*\n"
        f"⏱ Duration: *{duration}*\n"
        f"💰 Price:    *{price}*\n"
        f"🏦 Method:   *{pending['method']}*\n"
        f"🔐 TxID:     `{pending.get('txid', 'N/A')}`\n"
        f"{'─' * 30}\n"
        f"Review screenshot below 👇"
    )

    approve_cb = f"app_{user_id}_{pending['channel_id']}_{pending['mins']}"
    reject_cb  = f"rej_{user_id}"
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("✅ Approve", callback_data=approve_cb),
        InlineKeyboardButton("❌ Reject",  callback_data=reject_cb),
    )

    try:
        bot.send_photo(
            ADMIN_ID,
            photo_file_id,
            caption=caption,
            reply_markup=markup,
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error(f"Failed to send proof to admin: {e}")
        # Fallback: text only
        bot.send_message(
            ADMIN_ID,
            caption + f"\n\n📸 Screenshot file ID: `{photo_file_id}`",
            reply_markup=markup,
            parse_mode="Markdown",
        )


# ─────────────────────────────────────────────
# ADMIN: APPROVE PAYMENT
# ─────────────────────────────────────────────
@bot.callback_query_handler(func=lambda c: c.data.startswith("app_"))
def approve_user(call):
    if not is_admin(call.from_user.id):
        return bot.answer_callback_query(call.id, "❌ Unauthorized")
    try:
        parts   = call.data.split("_", 3)
        uid     = int(parts[1])
        ch_id   = int(parts[2])
        mins    = int(parts[3])
    except (ValueError, IndexError) as e:
        logger.error(f"Approve parse error: {e}")
        return bot.answer_callback_query(call.id, "❌ Parse error")

    expiry_dt = datetime.utcnow() + timedelta(minutes=mins)

    try:
        # Create a single-use invite link that expires when sub ends
        link = bot.create_chat_invite_link(
            ch_id,
            member_limit=1,
            expire_date=int(expiry_dt.timestamp()),
        )
        invite_url = link.invite_link
    except Exception as e:
        logger.error(f"Invite link error: {e}")
        invite_url = None

    # Save/update active subscription
    users_col.update_one(
        {"user_id": uid, "channel_id": ch_id},
        {
            "$set": {
                "user_id":    uid,
                "channel_id": ch_id,
                "mins":       mins,
                "expiry":     expiry_dt.timestamp(),
                "expiry_dt":  expiry_dt,
                "approved_at": datetime.utcnow(),
            }
        },
        upsert=True,
    )

    # Remove from pending
    pending_col.delete_one({"user_id": uid})

    duration = format_duration(mins)
    exp_str  = expiry_dt.strftime("%Y-%m-%d %H:%M UTC")

    if invite_url:
        user_msg = (
            f"🚀 *Access Approved!*\n\n"
            f"✅ Your payment has been verified.\n"
            f"⏱ Duration: *{duration}*\n"
            f"📅 Expires:  *{exp_str}*\n\n"
            f"🔗 *Your Invite Link* (single-use):\n{invite_url}\n\n"
            f"⚠️ This link expires on *{exp_str}*.\n"
            f"Join before it expires!"
        )
    else:
        user_msg = (
            f"🚀 *Access Approved!*\n\n"
            f"✅ Your payment has been verified.\n"
            f"⏱ Duration: *{duration}*\n"
            f"📅 Expires:  *{exp_str}*\n\n"
            f"⚠️ Could not generate invite link automatically.\n"
            f"Please contact {CONTACT_USERNAME} to get access."
        )

    try:
        bot.send_message(uid, user_msg, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Could not notify user {uid}: {e}")

    # Update admin message
    bot.edit_message_caption(
        caption=f"✅ *Approved!*\n\nUser `{uid}` approved for *{duration}*.\nExpires: *{exp_str}*",
        chat_id=call.message.chat.id,
        message_id=call.message.message_id,
        parse_mode="Markdown",
    )
    bot.answer_callback_query(call.id, f"✅ User {uid} approved!")
    logger.info(f"Approved user {uid} for channel {ch_id}, {mins} mins")


# ─────────────────────────────────────────────
# ADMIN: REJECT PAYMENT
# ─────────────────────────────────────────────
@bot.callback_query_handler(func=lambda c: c.data.startswith("rej_"))
def reject_user(call):
    if not is_admin(call.from_user.id):
        return bot.answer_callback_query(call.id, "❌ Unauthorized")
    try:
        uid = int(call.data.split("_", 1)[1])
    except (ValueError, IndexError):
        return bot.answer_callback_query(call.id, "❌ Parse error")

    # Remove pending proof
    pending_col.delete_one({"user_id": uid})

    try:
        bot.send_message(
            uid,
            f"❌ *Payment Rejected*\n\n"
            f"Your payment could not be verified. This may be due to:\n"
            f"• Invalid or unclear Transaction ID\n"
            f"• Incorrect payment amount\n"
            f"• Unreadable screenshot\n\n"
            f"Please try again or contact {CONTACT_USERNAME} for assistance.",
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error(f"Could not notify rejected user {uid}: {e}")

    bot.edit_message_caption(
        caption=f"❌ *Rejected*\n\nUser `{uid}`'s payment was rejected.",
        chat_id=call.message.chat.id,
        message_id=call.message.message_id,
        parse_mode="Markdown",
    )
    bot.answer_callback_query(call.id, f"❌ User {uid} rejected.")
    logger.info(f"Rejected payment for user {uid}")


# ─────────────────────────────────────────────
# BACKGROUND SCHEDULER: AUTO-KICK EXPIRED USERS
# ─────────────────────────────────────────────
def kick_expired_users():
    """Called every 60 seconds. Bans+unbans expired users and sends renewal message."""
    now       = datetime.utcnow().timestamp()
    expired   = list(users_col.find({"expiry": {"$lte": now}}))
    if not expired:
        return
    logger.info(f"⏰ Kicker: Found {len(expired)} expired subscription(s)")
    for record in expired:
        uid = record["user_id"]
        cid = record["channel_id"]
        try:
            # Ban (kick) the user
            bot.ban_chat_member(cid, uid)
            logger.info(f"  ✅ Banned user {uid} from channel {cid}")
        except Exception as e:
            logger.warning(f"  ⚠️  Could not ban {uid} from {cid}: {e}")

        try:
            # Immediately unban so they can rejoin via new invite link
            bot.unban_chat_member(cid, uid)
        except Exception as e:
            logger.warning(f"  ⚠️  Could not unban {uid} from {cid}: {e}")

        # Remove from active subscriptions DB
        users_col.delete_one({"_id": record["_id"]})

        # Fetch channel info for the renewal message
        ch_data  = channels_col.find_one({"channel_id": cid})
        ch_name  = ch_data["name"] if ch_data else f"Channel {cid}"
        bot_user = bot.get_me().username
        deep_link = f"https://t.me/{bot_user}?start={cid}"

        # Build renewal keyboard
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🔄 Renew Subscription", url=deep_link))
        markup.add(InlineKeyboardButton(f"💬 Contact Admin", url=f"https://t.me/{CONTACT_USERNAME.lstrip('@')}"))

        try:
            bot.send_message(
                uid,
                f"⏰ *Subscription Expired*\n\n"
                f"Your subscription to *{ch_name}* has ended.\n"
                f"You have been removed from the channel.\n\n"
                f"🔄 *Want to continue?*\nRenew your subscription using the button below!\n\n"
                f"Need help? Contact {CONTACT_USERNAME}",
                reply_markup=markup,
                parse_mode="Markdown",
            )
        except Exception as e:
            logger.warning(f"  ⚠️  Could not send renewal msg to {uid}: {e}")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    # Start Flask keep-alive in background thread
    Thread(target=run_web, daemon=True).start()
    logger.info("🌐 Flask keep-alive server started.")

    # Start APScheduler
    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(kick_expired_users, "interval", seconds=60, id="kicker")
    scheduler.start()
    logger.info("⏰ APScheduler started — checking expiries every 60 seconds.")

    logger.info("🤖 Bot starting polling...")
    try:
        bot.infinity_polling(timeout=30, long_polling_timeout=25)
    except Exception as e:
        logger.critical(f"Bot polling crashed: {e}")
    finally:
        scheduler.shutdown()
        logger.info("Scheduler shut down.")
