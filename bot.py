# =============================================================================
# Telegram Subscription Management Bot — v2 FULL REWRITE
# Fix: DB-backed state machine replaces register_next_step_handler
# Fix: Session-ID based callbacks (no more split("_") breakage)
# Fix: Live "Pending" status card for users
# Fix: All typos, auth guards, retry logic
# New: /mystatus, /pending, /kick, /broadcast, /stats, expiry warnings
# =============================================================================

import os
import time
import uuid
import logging
from datetime import datetime, timedelta
from threading import Thread

import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from pymongo import MongoClient, ASCENDING
from pymongo import errors as mongo_errors
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("SubBot")

# ─────────────────────────────────────────────────────────────────────────────
# FLASK KEEP-ALIVE  (required for Render free tier)
# ─────────────────────────────────────────────────────────────────────────────
flask_app = Flask(__name__)

@flask_app.route("/")
def index():
    return "✅ Subscription Bot is running!", 200

@flask_app.route("/health")
def health():
    try:
        db_client.admin.command("ping")
        db_ok = True
    except Exception:
        db_ok = False
    return jsonify(status="ok" if db_ok else "db_error",
                   db=db_ok,
                   ts=datetime.utcnow().isoformat()), 200 if db_ok else 503

def _run_flask():
    port = int(os.environ.get("PORT", 5000))
    flask_app.run(host="0.0.0.0", port=port, use_reloader=False)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
BOT_TOKEN        = os.getenv("BOT_TOKEN", "")
MONGO_URI        = os.getenv("MONGO_URI", "")
ADMIN_ID         = int(os.getenv("ADMIN_ID", "0"))
CONTACT_USERNAME = os.getenv("CONTACT_USERNAME", "@admin")

if not BOT_TOKEN or not MONGO_URI or ADMIN_ID == 0:
    raise SystemExit("❌ Missing env vars: BOT_TOKEN, MONGO_URI, ADMIN_ID")

# ─────────────────────────────────────────────────────────────────────────────
# MONGODB  —  connection with exponential-backoff retries
# ─────────────────────────────────────────────────────────────────────────────
def _mongo_connect(uri: str, retries: int = 6) -> MongoClient:
    delay = 2
    for attempt in range(1, retries + 1):
        try:
            c = MongoClient(uri,
                            serverSelectionTimeoutMS=6000,
                            connectTimeoutMS=6000,
                            socketTimeoutMS=12000)
            c.admin.command("ping")
            log.info("✅ MongoDB connected on attempt %d", attempt)
            return c
        except mongo_errors.ServerSelectionTimeoutError as e:
            log.warning("MongoDB attempt %d/%d failed: %s", attempt, retries, e)
            if attempt < retries:
                time.sleep(delay)
                delay = min(delay * 2, 30)
    raise SystemExit("❌ Could not connect to MongoDB after retries.")

db_client  = _mongo_connect(MONGO_URI)
db         = db_client["sub_mgmt"]

channels_col = db["channels"]    # registered private channels
gateways_col = db["gateways"]    # payment gateways (multi-currency)
users_col    = db["users"]       # active subscriptions
sessions_col = db["sessions"]    # payment flow sessions (state machine)
admin_col    = db["admin_state"] # admin conversation state machine

# Indexes
users_col.create_index([("expiry", ASCENDING)])
users_col.create_index([("user_id", ASCENDING), ("channel_id", ASCENDING)], unique=True)
sessions_col.create_index([("user_id", ASCENDING)], unique=True)
sessions_col.create_index([("created_at", ASCENDING)],
                          expireAfterSeconds=3600)   # auto-clean stale sessions
admin_col.create_index([("admin_id", ASCENDING)], unique=True)

log.info("✅ All DB indexes ensured.")

# ─────────────────────────────────────────────────────────────────────────────
# BOT INSTANCE
# ─────────────────────────────────────────────────────────────────────────────
bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID

def fmt_duration(minutes: int) -> str:
    """Convert raw minutes to human-readable string."""
    m = int(minutes)
    if m < 60:
        return f"{m} Min" + ("s" if m != 1 else "")
    elif m < 1440:
        h = m // 60
        return f"{h} Hour" + ("s" if h != 1 else "")
    elif m < 10080:
        d = m // 1440
        return f"{d} Day" + ("s" if d != 1 else "")
    elif m < 43200:
        w = m // 10080
        return f"{w} Week" + ("s" if w != 1 else "")
    else:
        mo = m // 43200
        return f"{mo} Month" + ("s" if mo != 1 else "")

def fmt_ts(ts: float) -> str:
    """Format a Unix timestamp for display."""
    return datetime.utcfromtimestamp(ts).strftime("%d %b %Y, %H:%M UTC")

def admin_panel_markup() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("📢 Add Channel",    callback_data="adm:add_ch"),
        InlineKeyboardButton("🏦 Add Gateway",    callback_data="adm:add_gw"),
        InlineKeyboardButton("📋 List Channels",  callback_data="adm:lst_ch"),
        InlineKeyboardButton("💳 List Gateways",  callback_data="adm:lst_gw"),
        InlineKeyboardButton("🔔 Pending Proofs", callback_data="adm:lst_pend"),
        InlineKeyboardButton("📊 Statistics",     callback_data="adm:stats"),
    )
    return kb

# ─────────────────────────────────────────────────────────────────────────────
# STATE MACHINE  —  USER  (stored in sessions_col)
#
# States:
#   awaiting_txid        → next message = transaction ID text
#   awaiting_screenshot  → next message = photo (screenshot)
#
# Sessions hold: user_id, channel_id, mins, method_name, currency,
#                txid, screenshot_file_id, status_msg_id, step
# ─────────────────────────────────────────────────────────────────────────────
def _set_user_state(user_id: int, state: str, extra: dict = None):
    doc = {"user_id": user_id, "step": state, "created_at": datetime.utcnow()}
    if extra:
        doc.update(extra)
    sessions_col.update_one({"user_id": user_id}, {"$set": doc}, upsert=True)

def _get_user_session(user_id: int) -> dict | None:
    return sessions_col.find_one({"user_id": user_id})

def _clear_user_state(user_id: int):
    sessions_col.delete_one({"user_id": user_id})

# ─────────────────────────────────────────────────────────────────────────────
# STATE MACHINE  —  ADMIN  (stored in admin_col)
#
# States:
#   add_ch_wait_forward  → waiting for forwarded channel message
#   add_ch_wait_plans    → waiting for plans text (extra: ch_id, ch_name)
#   add_gw_wait_input    → waiting for gateway CSV line
#   broadcast_wait_msg   → waiting for broadcast message text
#   delch_wait_name      → waiting for channel name to delete
#   delgw_wait_name      → waiting for gateway name to delete
#   kick_wait_input      → waiting for "user_id channel_id"
# ─────────────────────────────────────────────────────────────────────────────
def _set_admin_state(state: str, extra: dict = None):
    doc = {"admin_id": ADMIN_ID, "step": state, "ts": datetime.utcnow()}
    if extra:
        doc.update(extra)
    admin_col.update_one({"admin_id": ADMIN_ID}, {"$set": doc}, upsert=True)

def _get_admin_state() -> dict | None:
    return admin_col.find_one({"admin_id": ADMIN_ID})

def _clear_admin_state():
    admin_col.delete_one({"admin_id": ADMIN_ID})

# ─────────────────────────────────────────────────────────────────────────────
# UNIVERSAL MESSAGE ROUTER  —  THE KEY FIX
# All user messages pass through here; no register_next_step_handler used.
# ─────────────────────────────────────────────────────────────────────────────
@bot.message_handler(func=lambda m: True,
                     content_types=["text", "photo", "document", "audio",
                                    "video", "voice", "sticker"])
def universal_router(message):
    uid = message.from_user.id

    # ── Admin state machine ──────────────────────────────────────────────────
    if uid == ADMIN_ID:
        adm = _get_admin_state()
        if adm:
            step = adm.get("step", "")
            if step == "add_ch_wait_forward":
                return _handle_admin_ch_forward(message)
            elif step == "add_ch_wait_plans":
                return _handle_admin_ch_plans(message, adm)
            elif step == "add_gw_wait_input":
                return _handle_admin_gw_input(message)
            elif step == "broadcast_wait_msg":
                return _handle_admin_broadcast(message)
            elif step == "delch_wait_name":
                return _handle_admin_delch(message)
            elif step == "delgw_wait_name":
                return _handle_admin_delgw(message)
            elif step == "kick_wait_input":
                return _handle_admin_kick_input(message)

    # ── User state machine ───────────────────────────────────────────────────
    sess = _get_user_session(uid)
    if sess:
        step = sess.get("step", "")
        if step == "awaiting_txid":
            return _handle_user_txid(message, sess)
        elif step == "awaiting_screenshot":
            return _handle_user_screenshot(message, sess)

    # ── Commands ─────────────────────────────────────────────────────────────
    if message.content_type == "text" and message.text:
        text = message.text.strip()
        if text.startswith("/start"):
            return _cmd_start(message)
        elif text.startswith("/add") and uid == ADMIN_ID:
            return _cmd_add_channel(message)
        elif text.startswith("/gateway") and uid == ADMIN_ID:
            return _cmd_add_gateway(message)
        elif text.startswith("/channels") and uid == ADMIN_ID:
            return _list_channels(uid)
        elif text.startswith("/gateways") and uid == ADMIN_ID:
            return _list_gateways(uid)
        elif text.startswith("/pending") and uid == ADMIN_ID:
            return _cmd_pending(message)
        elif text.startswith("/stats") and uid == ADMIN_ID:
            return _cmd_stats(message)
        elif text.startswith("/broadcast") and uid == ADMIN_ID:
            return _cmd_broadcast(message)
        elif text.startswith("/kick") and uid == ADMIN_ID:
            return _cmd_kick(message)
        elif text.startswith("/delchannel") and uid == ADMIN_ID:
            return _cmd_delchannel(message)
        elif text.startswith("/delgateway") and uid == ADMIN_ID:
            return _cmd_delgateway(message)
        elif text.startswith("/mystatus"):
            return _cmd_mystatus(message)
        elif text.startswith("/cancel") and uid == ADMIN_ID:
            _clear_admin_state()
            bot.send_message(uid, "❌ Action cancelled.", parse_mode="Markdown")
        else:
            # Unknown message — show hint
            if uid != ADMIN_ID:
                bot.send_message(
                    uid,
                    "👋 Please use your subscription link to get started.\n"
                    f"Need help? {CONTACT_USERNAME}"
                )

# =============================================================================
# /start  —  entry point & deep-link handler
# =============================================================================
def _cmd_start(message):
    uid  = message.from_user.id
    args = message.text.split(maxsplit=1)

    if len(args) > 1:
        # Deep-link: ?start=
        try:
            ch_id   = int(args[1])
            ch_data = channels_col.find_one({"channel_id": ch_id})
            if not ch_data:
                return bot.send_message(
                    uid,
                    "❌ This channel link is invalid or no longer active.\n"
                    f"Contact {CONTACT_USERNAME}"
                )
            # Check if already subscribed
            existing = users_col.find_one({"user_id": uid, "channel_id": ch_id})
            if existing and existing.get("expiry", 0) > datetime.utcnow().timestamp():
                exp_str = fmt_ts(existing["expiry"])
                return bot.send_message(
                    uid,
                    f"✅ *You already have an active subscription!*\n\n"
                    f"📺 Channel: *{ch_data['name']}*\n"
                    f"📅 Expires: *{exp_str}*\n\n"
                    f"Use /mystatus to see all your subscriptions.",
                    parse_mode="Markdown"
                )
            plans  = ch_data.get("plans", {})
            if not plans:
                return bot.send_message(uid, "❌ No plans configured for this channel yet.")
            kb = InlineKeyboardMarkup(row_width=1)
            for mins_str, price in plans.items():
                label = fmt_duration(int(mins_str))
                kb.add(InlineKeyboardButton(
                    f"⏱ {label}  ➜  {price}",
                    callback_data=f"plan:{ch_id}:{mins_str}"
                ))
            bot.send_message(
                uid,
                f"💎 *{ch_data['name']}*\n\n"
                f"Choose your subscription plan 👇",
                reply_markup=kb,
                parse_mode="Markdown"
            )
            return
        except (ValueError, TypeError):
            pass

    # Admin panel
    if is_admin(uid):
        bot.send_message(
            uid,
            "🛠 *Admin Control Panel*\n\nWelcome back, Admin!",
            reply_markup=admin_panel_markup(),
            parse_mode="Markdown"
        )
    else:
        bot.send_message(
            uid,
            f"👋 *Hello!*\n\nTo subscribe, please use your channel link.\n"
            f"Need help? Contact {CONTACT_USERNAME}",
            parse_mode="Markdown"
        )

# =============================================================================
# PLAN SELECTION  →  currency selection
# =============================================================================
@bot.callback_query_handler(func=lambda c: c.data.startswith("plan:"))
def cb_plan(call):
    _, ch_id_s, mins_s = call.data.split(":", 2)
    ch_id = int(ch_id_s)
    ch_data = channels_col.find_one({"channel_id": ch_id})
    if not ch_data:
        return bot.answer_callback_query(call.id, "❌ Channel not found.")

    # Guard: already pending proof?
    sess = _get_user_session(call.from_user.id)
    if sess and sess.get("step") in ("awaiting_txid", "awaiting_screenshot"):
        return bot.answer_callback_query(
            call.id,
            "⏳ You already have a payment pending. Check your chat!", show_alert=True
        )

    currencies = gateways_col.distinct("currency")
    if not currencies:
        bot.answer_callback_query(call.id)
        return bot.send_message(
            call.message.chat.id,
            f"❌ No payment gateways set up yet.\nContact {CONTACT_USERNAME}"
        )

    price    = ch_data["plans"].get(mins_s, "N/A")
    duration = fmt_duration(int(mins_s))

    kb = InlineKeyboardMarkup(row_width=2)
    flags = {"BDT": "🇧🇩", "INR": "🇮🇳", "USD": "🇺🇸", "USDT": "💵",
             "EUR": "🇪🇺", "GBP": "🇬🇧", "PKR": "🇵🇰", "TRX": "⚡"}
    for cur in sorted(currencies):
        kb.add(InlineKeyboardButton(
            f"{flags.get(cur,'💱')} {cur}",
            callback_data=f"cur:{ch_id_s}:{mins_s}:{cur}"
        ))

    try:
        bot.edit_message_text(
            f"📦 *Plan Selected*\n"
            f"⏱ Duration: *{duration}*\n"
            f"💰 Price: *{price}*\n\n"
            f"🌍 Select your payment currency:",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=kb,
            parse_mode="Markdown"
        )
    except Exception:
        bot.send_message(
            call.message.chat.id,
            f"📦 *Plan:* {duration} — {price}\n\n🌍 Select currency:",
            reply_markup=kb,
            parse_mode="Markdown"
        )
    bot.answer_callback_query(call.id)

# =============================================================================
# CURRENCY SELECTION  →  method selection
# =============================================================================
@bot.callback_query_handler(func=lambda c: c.data.startswith("cur:"))
def cb_currency(call):
    parts   = call.data.split(":", 3)
    ch_id_s = parts[1]
    mins_s  = parts[2]
    cur     = parts[3]

    methods = list(gateways_col.find({"currency": cur}))
    if not methods:
        return bot.answer_callback_query(call.id, "❌ No methods for this currency.", show_alert=True)

    kb = InlineKeyboardMarkup(row_width=1)
    for m in methods:
        # Use MongoDB _id as safe key — avoids any special chars in method name
        kb.add(InlineKeyboardButton(
            f"💳 {m['method_name']}",
            callback_data=f"meth:{ch_id_s}:{mins_s}:{str(m['_id'])}"
        ))
    kb.add(InlineKeyboardButton("⬅️ Back", callback_data=f"plan:{ch_id_s}:{mins_s}"))

    try:
        bot.edit_message_text(
            f"💱 *Currency: {cur}*\n\nSelect payment method:",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=kb,
            parse_mode="Markdown"
        )
    except Exception:
        bot.send_message(
            call.message.chat.id,
            f"💱 {cur} — Select method:",
            reply_markup=kb,
            parse_mode="Markdown"
        )
    bot.answer_callback_query(call.id)

# =============================================================================
# METHOD SELECTION  →  show payment details
# =============================================================================
@bot.callback_query_handler(func=lambda c: c.data.startswith("meth:"))
def cb_method(call):
    from bson import ObjectId
    parts    = call.data.split(":", 3)
    ch_id_s  = parts[1]
    mins_s   = parts[2]
    gw_id_s  = parts[3]

    try:
        gw = gateways_col.find_one({"_id": ObjectId(gw_id_s)})
    except Exception:
        return bot.answer_callback_query(call.id, "❌ Gateway not found.", show_alert=True)
    if not gw:
        return bot.answer_callback_query(call.id, "❌ Gateway removed.", show_alert=True)

    ch_id   = int(ch_id_s)
    ch_data = channels_col.find_one({"channel_id": ch_id})
    if not ch_data:
        return bot.answer_callback_query(call.id, "❌ Channel not found.", show_alert=True)

    price    = ch_data["plans"].get(mins_s, "N/A")
    duration = fmt_duration(int(mins_s))

    text = (
        f"📋 *Payment Instructions*\n"
        f"{'━'*30}\n"
        f"📺 Channel:  *{ch_data['name']}*\n"
        f"⏱ Duration: *{duration}*\n"
        f"💰 Amount:   *{price}*\n"
        f"{'━'*30}\n"
        f"🏦 Method:   *{gw['method_name']}*\n"
        f"💱 Currency: *{gw['currency']}*\n"
        f"📬 Send to:  `{gw['details']}`\n\n"
        f"📝 *Instructions:*\n{gw['instructions']}\n"
        f"{'━'*30}\n"
        f"After paying, tap ✅ *I Have Paid* and submit your proof."
    )
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton(
            "✅ I Have Paid — Submit Proof",
            callback_data=f"paid:{ch_id_s}:{mins_s}:{gw_id_s}"
        ),
        InlineKeyboardButton("⬅️ Back", callback_data=f"cur:{ch_id_s}:{mins_s}:{gw['currency']}")
    )

    try:
        bot.edit_message_text(
            text, call.message.chat.id, call.message.message_id,
            reply_markup=kb, parse_mode="Markdown"
        )
    except Exception:
        bot.send_message(
            call.message.chat.id, text, reply_markup=kb, parse_mode="Markdown"
        )
    bot.answer_callback_query(call.id)

# =============================================================================
# "I HAVE PAID" BUTTON  →  begin proof collection (STATE MACHINE)
# =============================================================================
@bot.callback_query_handler(func=lambda c: c.data.startswith("paid:"))
def cb_paid(call):
    from bson import ObjectId
    parts   = call.data.split(":", 3)
    ch_id_s = parts[1]
    mins_s  = parts[2]
    gw_id_s = parts[3]
    uid     = call.from_user.id

    try:
        gw = gateways_col.find_one({"_id": ObjectId(gw_id_s)})
    except Exception:
        return bot.answer_callback_query(call.id, "❌ Gateway error.", show_alert=True)

    ch_data = channels_col.find_one({"channel_id": int(ch_id_s)})
    if not ch_data or not gw:
        return bot.answer_callback_query(call.id, "❌ Data error.", show_alert=True)

    # Guard: already has a pending session?
    existing = _get_user_session(uid)
    if existing and existing.get("step") in ("awaiting_txid","awaiting_screenshot"):
        bot.answer_callback_query(call.id, "⏳ You already submitted a proof. Please wait!", show_alert=True)
        return

    # Save session state → awaiting_txid
    _set_user_state(uid, "awaiting_txid", {
        "channel_id":  int(ch_id_s),
        "mins":        mins_s,
        "gw_id":       gw_id_s,
        "method_name": gw["method_name"],
        "currency":    gw["currency"],
        "ch_name":     ch_data["name"],
        "price":       ch_data["plans"].get(mins_s, "N/A"),
        "first_name":  call.from_user.first_name or "",
        "username":    call.from_user.username or "",
    })

    bot.answer_callback_query(call.id, "✅ Starting proof submission...")

    # Delete the payment details message to keep chat clean
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception:
        pass

    bot.send_message(
        uid,
        "🔐 *Payment Proof — Step 1 of 2*\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Please send your *Transaction ID* (TxID) or *Reference Number* exactly as shown.\n\n"
        "📌 Examples:\n"
        "`TXN8273640192`\n`BK240501ABC`\n`UPI/123456789012`\n\n"
        "⌨️ Type and send it as a plain text message now:",
        parse_mode="Markdown"
    )

# =============================================================================
# STATE HANDLER: awaiting_txid  (called from universal_router)
# =============================================================================
def _handle_user_txid(message, sess: dict):
    uid = message.from_user.id

    if message.content_type != "text" or not message.text:
        return bot.send_message(
            uid,
            "❌ Please send your *Transaction ID as text*, not a photo or file.\n\n"
            "Type your TxID and send it as a plain message.",
            parse_mode="Markdown"
        )

    txid = message.text.strip()
    if len(txid) < 3:
        return bot.send_message(
            uid,
            "❌ Transaction ID seems too short. Please double-check and resend.",
            parse_mode="Markdown"
        )

    # Save TxID and advance state
    _set_user_state(uid, "awaiting_screenshot", {"txid": txid})

    bot.send_message(
        uid,
        f"✅ *TxID saved:* `{txid}`\n\n"
        f"🔐 *Payment Proof — Step 2 of 2*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Now send a *screenshot* of your payment confirmation.\n\n"
        f"📌 Important:\n"
        f"• Send as a *Photo* (tap 📎 → Photo/Gallery)\n"
        f"• Do NOT send as a File or Document\n"
        f"• The screenshot must clearly show the amount & TxID",
        parse_mode="Markdown"
    )

# =============================================================================
# STATE HANDLER: awaiting_screenshot  (called from universal_router)
# =============================================================================
def _handle_user_screenshot(message, sess: dict):
    uid = message.from_user.id

    # Reject non-photo
    if message.content_type != "photo" or not message.photo:
        hint = ""
        if message.content_type == "document":
            hint = "\n\n⚠️ You sent a *file/document*. Please resend as a *Photo*."
        return bot.send_message(
            uid,
            f"❌ *Please send a Photo, not a {message.content_type}.*{hint}\n\n"
            f"Tap 📎 → choose *Photo* from gallery and send.",
            parse_mode="Markdown"
        )

    photo_file_id = message.photo[-1].file_id  # highest quality

    # Mark session as submitted
    sessions_col.update_one(
        {"user_id": uid},
        {"$set": {
            "screenshot_file_id": photo_file_id,
            "step":               "submitted",
            "submitted_at":       datetime.utcnow(),
        }}
    )

    # ── Send "Waiting" status card to user ─────────────────────────────────
    duration = fmt_duration(int(sess["mins"]))
    status_msg = bot.send_message(
        uid,
        f"⏳ *Payment Under Review*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📺 Channel:  *{sess['ch_name']}*\n"
        f"⏱ Duration: *{duration}*\n"
        f"💰 Amount:   *{sess['price']}*\n"
        f"🏦 Method:   *{sess['method_name']}*\n"
        f"🔐 TxID:     `{sess.get('txid','N/A')}`\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 *Status: PENDING REVIEW*\n\n"
        f"Your proof has been submitted and is being reviewed by the admin.\n"
        f"You will be notified here once approved or rejected.\n\n"
        f"⏱ Average review time: 5–30 minutes.\n"
        f"Need help? {CONTACT_USERNAME}",
        parse_mode="Markdown"
    )

    # Save the status message ID so we can edit it later on approval/rejection
    sessions_col.update_one(
        {"user_id": uid},
        {"$set": {"status_msg_id": status_msg.message_id}}
    )

    # ── Notify admin with full proof ────────────────────────────────────────
    user_tag = (
        f"@{sess['username']}" if sess.get("username")
        else sess.get("first_name", "Unknown")
    )
    caption = (
        f"🔔 *New Payment Proof*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 User:     {user_tag} (`{uid}`)\n"
        f"📺 Channel:  *{sess['ch_name']}*\n"
        f"⏱ Duration: *{duration}*\n"
        f"💰 Amount:   *{sess['price']}*\n"
        f"🏦 Method:   *{sess['method_name']}*\n"
        f"💱 Currency: *{sess['currency']}*\n"
        f"🔐 TxID:     `{sess.get('txid','N/A')}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Review screenshot 👆 and approve or reject:"
    )

    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Approve", callback_data=f"app:{uid}"),
        InlineKeyboardButton("❌ Reject",  callback_data=f"rej:{uid}")
    )

    try:
        bot.send_photo(
            ADMIN_ID,
            photo=photo_file_id,
            caption=caption,
            reply_markup=kb,
            parse_mode="Markdown"
        )
        log.info("Proof forwarded to admin for user %d", uid)
    except Exception as e:
        log.error("Failed to send proof photo to admin: %s", e)
        # Fallback text message
        bot.send_message(
            ADMIN_ID,
            caption + f"\n\n📸 Screenshot FileID: `{photo_file_id}`",
            reply_markup=kb,
            parse_mode="Markdown"
        )

# =============================================================================
# ADMIN: APPROVE
# =============================================================================
@bot.callback_query_handler(func=lambda c: c.data.startswith("app:"))
def cb_approve(call):
    if not is_admin(call.from_user.id):
        return bot.answer_callback_query(call.id, "❌ Unauthorized")

    uid = int(call.data.split(":", 1)[1])
    sess = sessions_col.find_one({"user_id": uid})
    if not sess:
        bot.answer_callback_query(call.id, "⚠️ Session expired or already processed.", show_alert=True)
        return bot.edit_message_caption(
            "⚠️ Session already processed.", call.message.chat.id, call.message.message_id
        )

    ch_id    = sess["channel_id"]
    mins     = int(sess["mins"])
    duration = fmt_duration(mins)
    expiry_dt = datetime.utcnow() + timedelta(minutes=mins)

    # Create single-use invite link expiring with the subscription
    try:
        link_obj  = bot.create_chat_invite_link(
            ch_id, member_limit=1, expire_date=int(expiry_dt.timestamp())
        )
        invite_url = link_obj.invite_link
    except Exception as e:
        log.error("Invite link creation failed: %s", e)
        invite_url = None

    # Upsert active subscription
    try:
        users_col.update_one(
            {"user_id": uid, "channel_id": ch_id},
            {"$set": {
                "user_id":     uid,
                "channel_id":  ch_id,
                "mins":        mins,
                "ch_name":     sess.get("ch_name",""),
                "expiry":      expiry_dt.timestamp(),
                "expiry_dt":   expiry_dt,
                "approved_at": datetime.utcnow(),
                "method":      sess.get("method_name",""),
                "txid":        sess.get("txid",""),
            }},
            upsert=True
        )
    except Exception as e:
        log.error("DB upsert error on approve: %s", e)

    # Clean up session
    status_msg_id = sess.get("status_msg_id")
    _clear_user_state(uid)

    exp_str = fmt_ts(expiry_dt.timestamp())

    # ── Update user's "Pending" status card ──────────────────────────────
    if status_msg_id:
        ch_data = channels_col.find_one({"channel_id": ch_id})
        bot_user = bot.get_me().username
        deep_link = f"https://t.me/{bot_user}?start={ch_id}"
        try:
            bot.edit_message_text(
                f"✅ *Payment Approved!*\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📺 Channel:  *{sess.get('ch_name','')}*\n"
                f"⏱ Duration: *{duration}*\n"
                f"💰 Amount:   *{sess.get('price','N/A')}*\n"
                f"📅 Expires:  *{exp_str}*\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🟢 *Status: APPROVED*",
                uid,
                status_msg_id,
                parse_mode="Markdown"
            )
        except Exception as e:
            log.warning("Could not edit user status card: %s", e)

    # ── Send invite link to user ──────────────────────────────────────────
    if invite_url:
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("🚀 Join Channel Now", url=invite_url))
        bot.send_message(
            uid,
            f"🎉 *Access Granted!*\n\n"
            f"Your subscription to *{sess.get('ch_name','')}* is now active.\n\n"
            f"📅 *Expires:* {exp_str}\n\n"
            f"🔗 *Single-use invite link below ↓*\n"
            f"⚠️ Use it immediately — it can only be used *once*!",
            reply_markup=kb,
            parse_mode="Markdown"
        )
    else:
        bot.send_message(
            uid,
            f"✅ *Approved!*\n\nCould not auto-generate a link.\n"
            f"Please contact {CONTACT_USERNAME} to get access.",
            parse_mode="Markdown"
        )

    # ── Update admin's proof message ──────────────────────────────────────
    new_cap = (
        call.message.caption or ""
    ) + f"\n\n✅ *APPROVED* by admin\n📅 Expires: {exp_str}"
    try:
        bot.edit_message_caption(
            new_cap, call.message.chat.id, call.message.message_id,
            parse_mode="Markdown"
        )
    except Exception:
        pass

    bot.answer_callback_query(call.id, f"✅ User {uid} approved for {duration}!")
    log.info("Approved user %d → channel %d for %d mins", uid, ch_id, mins)

# =============================================================================
# ADMIN: REJECT
# =============================================================================
@bot.callback_query_handler(func=lambda c: c.data.startswith("rej:"))
def cb_reject(call):
    if not is_admin(call.from_user.id):
        return bot.answer_callback_query(call.id, "❌ Unauthorized")

    uid = int(call.data.split(":", 1)[1])
    sess = sessions_col.find_one({"user_id": uid})

    status_msg_id = sess.get("status_msg_id") if sess else None
    _clear_user_state(uid)

    # ── Update user's status card ─────────────────────────────────────────
    if status_msg_id:
        try:
            bot.edit_message_text(
                f"❌ *Payment Rejected*\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🔴 *Status: REJECTED*\n\n"
                f"Your payment proof could not be verified.\n"
                f"Please check your TxID and screenshot and try again.",
                uid,
                status_msg_id,
                parse_mode="Markdown"
            )
        except Exception as e:
            log.warning("Could not edit user status card on reject: %s", e)

    # ── Notify user ───────────────────────────────────────────────────────
    ch_data = channels_col.find_one({"channel_id": sess["channel_id"]}) if sess else None
    bot_user = bot.get_me().username
    deep_link = f"https://t.me/{bot_user}?start={sess['channel_id']}" if sess else ""
    kb = InlineKeyboardMarkup()
    if deep_link:
        kb.add(InlineKeyboardButton("🔄 Try Again", url=deep_link))
    kb.add(InlineKeyboardButton("💬 Contact Admin", url=f"https://t.me/{CONTACT_USERNAME.lstrip('@')}"))

    try:
        bot.send_message(
            uid,
            f"❌ *Payment Rejected*\n\n"
            f"Your payment could not be verified. Common reasons:\n"
            f"• Invalid or incorrect Transaction ID\n"
            f"• Wrong payment amount\n"
            f"• Blurry / incorrect screenshot\n"
            f"• Payment sent to wrong account\n\n"
            f"Please try again or contact {CONTACT_USERNAME}.",
            reply_markup=kb,
            parse_mode="Markdown"
        )
    except Exception as e:
        log.error("Could not notify rejected user %d: %s", uid, e)

    new_cap = (call.message.caption or "") + f"\n\n❌ *REJECTED* by admin"
    try:
        bot.edit_message_caption(
            new_cap, call.message.chat.id, call.message.message_id,
            parse_mode="Markdown"
        )
    except Exception:
        pass

    bot.answer_callback_query(call.id, f"❌ User {uid} rejected.")
    log.info("Rejected payment for user %d", uid)

# =============================================================================
# ADMIN CALLBACKS  (panel buttons)
# =============================================================================
@bot.callback_query_handler(func=lambda c: c.data.startswith("adm:"))
def cb_admin_panel(call):
    if not is_admin(call.from_user.id):
        return bot.answer_callback_query(call.id, "❌ Unauthorized")
    bot.answer_callback_query(call.id)
    action = call.data.split(":", 1)[1]

    if action == "add_ch":
        _prompt_add_channel(call.message.chat.id)
    elif action == "add_gw":
        _prompt_add_gateway(call.message.chat.id)
    elif action == "lst_ch":
        _list_channels(call.message.chat.id)
    elif action == "lst_gw":
        _list_gateways(call.message.chat.id)
    elif action == "lst_pend":
        _list_pending(call.message.chat.id)
    elif action == "stats":
        _show_stats(call.message.chat.id)

# =============================================================================
# ADMIN: ADD CHANNEL
# =============================================================================
def _cmd_add_channel(message):
    _prompt_add_channel(message.chat.id)

def _prompt_add_channel(chat_id):
    _set_admin_state("add_ch_wait_forward")
    bot.send_message(
        chat_id,
        "📢 *Add New Channel*\n\n"
        "Forward any message from your *private channel* to me.\n\n"
        "⚠️ The bot must be an *admin* in that channel with "
        "*Invite Users* permission.\n\n"
        "Send /cancel to abort.",
        parse_mode="Markdown"
    )

def _handle_admin_ch_forward(message):
    if not message.forward_from_chat:
        return bot.send_message(
            ADMIN_ID,
            "❌ That is not a forwarded channel message. "
            "Please forward a message *from the private channel*.\n\nSend /cancel to abort.",
            parse_mode="Markdown"
        )
    ch_id   = message.forward_from_chat.id
    ch_name = message.forward_from_chat.title or f"Channel {ch_id}"
    _set_admin_state("add_ch_wait_plans", {"ch_id": ch_id, "ch_name": ch_name})
    bot.send_message(
        ADMIN_ID,
        f"✅ Detected: *{ch_name}*\n\n"
        f"Now send the subscription plans:\n"
        f"`Minutes:Price, Minutes:Price, ...`\n\n"
        f"📌 Examples:\n"
        f"`1440:100 BDT, 43200:500 BDT, 525600:2000 BDT`\n"
        f"`60:5 USD, 1440:20 USD, 43200:50 USD`\n\n"
        f"Send /cancel to abort.",
        parse_mode="Markdown"
    )

def _handle_admin_ch_plans(message, adm_state: dict):
    if not message.text:
        return bot.send_message(ADMIN_ID, "❌ Please send the plans as text.")
    try:
        raw   = message.text.strip().split(",")
        plans = {}
        for entry in raw:
            entry = entry.strip()
            colon = entry.index(":")
            mins  = entry[:colon].strip()
            price = entry[colon+1:].strip()
            if not mins.isdigit():
                raise ValueError(f"'{mins}' is not a number")
            plans[mins] = price
        if not plans:
            raise ValueError("Empty plans")
    except Exception as e:
        return bot.send_message(
            ADMIN_ID,
            f"❌ Format error: {e}\n\n"
            f"Use: `1440:100 BDT, 43200:500 BDT`",
            parse_mode="Markdown"
        )

    ch_id   = adm_state["ch_id"]
    ch_name = adm_state["ch_name"]
    channels_col.update_one(
        {"channel_id": ch_id},
        {"$set": {"channel_id": ch_id, "name": ch_name,
                  "plans": plans, "updated_at": datetime.utcnow()}},
        upsert=True
    )
    _clear_admin_state()

    bot_user  = bot.get_me().username
    deep_link = f"https://t.me/{bot_user}?start={ch_id}"
    plans_txt = "\n".join(
        f"  • {fmt_duration(int(m))} → {p}" for m, p in plans.items()
    )
    bot.send_message(
        ADMIN_ID,
        f"✅ *Channel Registered!*\n\n"
        f"📢 *{ch_name}*\n\n"
        f"📦 Plans:\n{plans_txt}\n\n"
        f"🔗 Share link:\n`{deep_link}`",
        reply_markup=admin_panel_markup(),
        parse_mode="Markdown"
    )

# =============================================================================
# ADMIN: ADD GATEWAY
# =============================================================================
def _cmd_add_gateway(message):
    _prompt_add_gateway(message.chat.id)

def _prompt_add_gateway(chat_id):
    _set_admin_state("add_gw_wait_input")
    bot.send_message(
        chat_id,
        "🏦 *Add Payment Gateway*\n\n"
        "Send in this format (comma-separated):\n"
        "`Currency, MethodName, SendTo, Instructions`\n\n"
        "📌 Examples:\n"
        "`BDT, bKash, 01712345678, Send Money — use number above`\n"
        "`INR, UPI, merchant@ybl, Pay via any UPI app`\n"
        "`USD, PayPal, pay@mail.com, Friends & Family only`\n"
        "`USDT, Binance TRC20, TAddr123, TRC20 network only`\n\n"
        "Send /cancel to abort.",
        parse_mode="Markdown"
    )

def _handle_admin_gw_input(message):
    if not message.text:
        return bot.send_message(ADMIN_ID, "❌ Please send text.")
    try:
        parts = [x.strip() for x in message.text.split(",", 3)]
        if len(parts) != 4:
            raise ValueError("Need exactly 4 comma-separated fields")
        curr, method, details, instructions = parts
        if not curr or not method:
            raise ValueError("Currency and Method cannot be empty")
    except Exception as e:
        return bot.send_message(
            ADMIN_ID,
            f"❌ Error: {e}\n\nFormat: `Currency, Method, Details, Instructions`",
            parse_mode="Markdown"
        )

    gateways_col.update_one(
        {"method_name": method},
        {"$set": {
            "currency":     curr.upper(),
            "method_name":  method,
            "details":      details,
            "instructions": instructions,
            "updated_at":   datetime.utcnow(),
        }},
        upsert=True
    )
    _clear_admin_state()
    bot.send_message(
        ADMIN_ID,
        f"✅ *Gateway Saved!*\n\n"
        f"💱 Currency: `{curr.upper()}`\n"
        f"🏦 Method:   `{method}`\n"
        f"📬 Details:  `{details}`",
        reply_markup=admin_panel_markup(),
        parse_mode="Markdown"
    )

# =============================================================================
# ADMIN: LIST CHANNELS
# =============================================================================
def _cmd_add_channel_stub(message):
    if is_admin(message.from_user.id):
        _list_channels(message.chat.id)

def _list_channels(chat_id):
    chs = list(channels_col.find())
    if not chs:
        return bot.send_message(chat_id, "📭 No channels registered yet.")
    bot_user = bot.get_me().username
    lines = ["📋 *Registered Channels*\n"]
    for i, ch in enumerate(chs, 1):
        link  = f"https://t.me/{bot_user}?start={ch['channel_id']}"
        plans = "\n".join(
            f"    • {fmt_duration(int(m))} → {p}"
            for m, p in ch.get("plans", {}).items()
        )
        lines.append(
            f"*{i}. {ch['name']}*\n"
            f"   🆔 `{ch['channel_id']}`\n"
            f"   🔗 `{link}`\n"
            f"   📦 Plans:\n{plans}\n"
        )
    bot.send_message(chat_id, "\n".join(lines), parse_mode="Markdown")

# =============================================================================
# ADMIN: LIST GATEWAYS
# =============================================================================
def _list_gateways(chat_id):
    gws = list(gateways_col.find())
    if not gws:
        return bot.send_message(chat_id, "📭 No gateways registered yet.")
    lines = ["🏦 *Payment Gateways*\n"]
    for i, gw in enumerate(gws, 1):
        lines.append(
            f"*{i}. {gw['method_name']}*\n"
            f"   💱 {gw['currency']}\n"
            f"   📬 `{gw['details']}`\n"
            f"   📝 {gw['instructions']}\n"
        )
    bot.send_message(chat_id, "\n".join(lines), parse_mode="Markdown")

# =============================================================================
# ADMIN: LIST PENDING
# =============================================================================
def _cmd_pending(message):
    _list_pending(message.chat.id)

def _list_pending(chat_id):
    pending = list(sessions_col.find({"step": "submitted"}))
    if not pending:
        return bot.send_message(chat_id, "✅ No pending payment proofs right now.")
    bot.send_message(
        chat_id,
        f"🔔 *{len(pending)} Pending Proof(s)*\n\n"
        "Use Approve/Reject buttons on the original proof messages, or see below:",
        parse_mode="Markdown"
    )
    for sess in pending:
        uid      = sess["user_id"]
        duration = fmt_duration(int(sess["mins"]))
        user_tag = f"@{sess['username']}" if sess.get("username") else sess.get("first_name","?")
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("✅ Approve", callback_data=f"app:{uid}"),
            InlineKeyboardButton("❌ Reject",  callback_data=f"rej:{uid}")
        )
        msg = (
            f"👤 {user_tag} (`{uid}`)\n"
            f"📺 {sess.get('ch_name','?')}\n"
            f"⏱ {duration} — {sess.get('price','?')}\n"
            f"🏦 {sess.get('method_name','?')}\n"
            f"🔐 TxID: `{sess.get('txid','?')}`"
        )
        if sess.get("screenshot_file_id"):
            try:
                bot.send_photo(
                    chat_id,
                    sess["screenshot_file_id"],
                    caption=msg,
                    reply_markup=kb,
                    parse_mode="Markdown"
                )
                continue
            except Exception:
                pass
        bot.send_message(chat_id, msg, reply_markup=kb, parse_mode="Markdown")

# =============================================================================
# ADMIN: STATS
# =============================================================================
def _cmd_stats(message):
    _show_stats(message.chat.id)

def _show_stats(chat_id):
    now         = datetime.utcnow().timestamp()
    total_active = users_col.count_documents({"expiry": {"$gt": now}})
    total_expired = users_col.count_documents({"expiry": {"$lte": now}})
    total_pending = sessions_col.count_documents({"step": "submitted"})
    total_ch      = channels_col.count_documents({})
    total_gw      = gateways_col.count_documents({})

    # Per-channel breakdown
    ch_lines = []
    for ch in channels_col.find():
        count = users_col.count_documents({
            "channel_id": ch["channel_id"],
            "expiry": {"$gt": now}
        })
        ch_lines.append(f"  • {ch['name']}: *{count}* active")

    bot.send_message(
        chat_id,
        f"📊 *Bot Statistics*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🟢 Active Subs:   *{total_active}*\n"
        f"🔴 Expired Subs:  *{total_expired}*\n"
        f"⏳ Pending Proofs: *{total_pending}*\n"
        f"📢 Channels:      *{total_ch}*\n"
        f"🏦 Gateways:      *{total_gw}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"*Per Channel (active):*\n" +
        ("\n".join(ch_lines) if ch_lines else "  No channels"),
        parse_mode="Markdown"
    )

# =============================================================================
# ADMIN: BROADCAST
# =============================================================================
def _cmd_broadcast(message):
    _set_admin_state("broadcast_wait_msg")
    bot.send_message(
        ADMIN_ID,
        "📣 *Broadcast Message*\n\n"
        "Send the message you want to broadcast to ALL active subscribers.\n"
        "Supports Markdown formatting.\n\n"
        "Send /cancel to abort.",
        parse_mode="Markdown"
    )

def _handle_admin_broadcast(message):
    if not message.text:
        return bot.send_message(ADMIN_ID, "❌ Please send a text message.")
    _clear_admin_state()
    now   = datetime.utcnow().timestamp()
    subs  = users_col.find({"expiry": {"$gt": now}})
    sent  = 0
    fails = 0
    for sub in subs:
        try:
            bot.send_message(
                sub["user_id"],
                f"📣 *Announcement*\n\n{message.text}",
                parse_mode="Markdown"
            )
            sent += 1
            time.sleep(0.05)  # Avoid flood
        except Exception:
            fails += 1
    bot.send_message(
        ADMIN_ID,
        f"📣 *Broadcast Done*\n\n✅ Sent: {sent}\n❌ Failed: {fails}",
        reply_markup=admin_panel_markup(),
        parse_mode="Markdown"
    )

# =============================================================================
# ADMIN: MANUAL KICK
# =============================================================================
def _cmd_kick(message):
    args = message.text.split()[1:]
    if len(args) == 2:
        # Inline: /kick user_id channel_id
        try:
            uid = int(args[0])
            cid = int(args[1])
            _do_kick(uid, cid)
            bot.send_message(ADMIN_ID, f"✅ Kicked user `{uid}` from `{cid}`.",
                             parse_mode="Markdown")
        except Exception as e:
            bot.send_message(ADMIN_ID, f"❌ Error: {e}")
    else:
        _set_admin_state("kick_wait_input")
        bot.send_message(
            ADMIN_ID,
            "⚠️ *Manual Kick*\n\n"
            "Send: `user_id channel_id`\n"
            "Example: `123456789 -1001234567890`\n\n"
            "Send /cancel to abort.",
            parse_mode="Markdown"
        )

def _handle_admin_kick_input(message):
    if not message.text:
        return bot.send_message(ADMIN_ID, "❌ Please send text.")
    try:
        parts = message.text.strip().split()
        uid   = int(parts[0])
        cid   = int(parts[1])
    except Exception:
        return bot.send_message(
            ADMIN_ID, "❌ Format: `user_id channel_id`", parse_mode="Markdown"
        )
    _clear_admin_state()
    _do_kick(uid, cid)
    bot.send_message(
        ADMIN_ID, f"✅ Kicked user `{uid}` from `{cid}`.",
        reply_markup=admin_panel_markup(), parse_mode="Markdown"
    )

def _do_kick(uid: int, cid: int):
    try:
        bot.ban_chat_member(cid, uid)
        time.sleep(0.5)
        bot.unban_chat_member(cid, uid)
    except Exception as e:
        log.warning("Kick error for %d/%d: %s", uid, cid, e)
    users_col.delete_one({"user_id": uid, "channel_id": cid})

# =============================================================================
# ADMIN: DELETE CHANNEL
# =============================================================================
def _cmd_delchannel(message):
    chs = list(channels_col.find({}, {"name": 1, "channel_id": 1}))
    if not chs:
        return bot.send_message(ADMIN_ID, "📭 No channels to delete.")
    _set_admin_state("delch_wait_name")
    names = "\n".join(f"• {ch['name']}" for ch in chs)
    bot.send_message(
        ADMIN_ID,
        f"🗑 *Delete Channel*\n\nExisting channels:\n{names}\n\n"
        f"Send the exact channel name to delete.\nSend /cancel to abort.",
        parse_mode="Markdown"
    )

def _handle_admin_delch(message):
    if not message.text:
        return bot.send_message(ADMIN_ID, "❌ Send the channel name.")
    name = message.text.strip()
    result = channels_col.delete_one({"name": name})
    _clear_admin_state()
    if result.deleted_count:
        bot.send_message(
            ADMIN_ID, f"✅ Channel *{name}* deleted.",
            reply_markup=admin_panel_markup(), parse_mode="Markdown"
        )
    else:
        bot.send_message(
            ADMIN_ID, f"❌ No channel named *{name}* found.",
            reply_markup=admin_panel_markup(), parse_mode="Markdown"
        )

# =============================================================================
# ADMIN: DELETE GATEWAY
# =============================================================================
def _cmd_delgateway(message):
    gws = list(gateways_col.find({}, {"method_name": 1}))
    if not gws:
        return bot.send_message(ADMIN_ID, "📭 No gateways to delete.")
    _set_admin_state("delgw_wait_name")
    names = "\n".join(f"• {gw['method_name']}" for gw in gws)
    bot.send_message(
        ADMIN_ID,
        f"🗑 *Delete Gateway*\n\nExisting gateways:\n{names}\n\n"
        f"Send the exact method name to delete.\nSend /cancel to abort.",
        parse_mode="Markdown"
    )

def _handle_admin_delgw(message):
    if not message.text:
        return bot.send_message(ADMIN_ID, "❌ Send the gateway name.")
    name = message.text.strip()
    result = gateways_col.delete_one({"method_name": name})
    _clear_admin_state()
    if result.deleted_count:
        bot.send_message(
            ADMIN_ID, f"✅ Gateway *{name}* deleted.",
            reply_markup=admin_panel_markup(), parse_mode="Markdown"
        )
    else:
        bot.send_message(
            ADMIN_ID, f"❌ No gateway named *{name}* found.",
            reply_markup=admin_panel_markup(), parse_mode="Markdown"
        )

# =============================================================================
# USER: /mystatus
# =============================================================================
def _cmd_mystatus(message):
    uid  = message.from_user.id
    now  = datetime.utcnow().timestamp()
    subs = list(users_col.find({"user_id": uid, "expiry": {"$gt": now}}))
    pending = sessions_col.find_one({"user_id": uid})

    if not subs and not pending:
        return bot.send_message(
            uid,
            "📭 *No Active Subscriptions*\n\n"
            "You have no active subscriptions and no pending payments.\n"
            f"Contact {CONTACT_USERNAME} for help.",
            parse_mode="Markdown"
        )

    lines = ["📋 *Your Subscriptions*\n"]
    for sub in subs:
        remaining_s = sub["expiry"] - now
        remaining_h = remaining_s / 3600
        if remaining_h < 24:
            rem_str = f"⚠️ {remaining_h:.1f} hours left"
        else:
            rem_str = f"{remaining_h/24:.1f} days left"
        lines.append(
            f"📺 *{sub.get('ch_name','Channel')}*\n"
            f"   📅 Expires: {fmt_ts(sub['expiry'])}\n"
            f"   ⏳ {rem_str}\n"
        )

    if pending and pending.get("step") in ("awaiting_txid","awaiting_screenshot","submitted"):
        lines.append(
            f"\n⏳ *Pending Payment*\n"
            f"   Channel: {pending.get('ch_name','?')}\n"
            f"   Status: {pending.get('step','?').replace('_',' ').title()}"
        )

    bot.send_message(uid, "\n".join(lines), parse_mode="Markdown")

# =============================================================================
# BACKGROUND SCHEDULER JOBS
# =============================================================================
def _job_kick_expired():
    """Main expiry checker — runs every 60 seconds."""
    now     = datetime.utcnow().timestamp()
    expired = list(users_col.find({"expiry": {"$lte": now}}))
    if not expired:
        return
    log.info("⏰ Kicker: %d expired subscription(s) found", len(expired))

    for rec in expired:
        uid = rec["user_id"]
        cid = rec["channel_id"]
        _do_kick(uid, cid)

        # Send renewal notification
        ch_data   = channels_col.find_one({"channel_id": cid})
        ch_name   = ch_data["name"] if ch_data else str(cid)
        bot_user  = bot.get_me().username
        deep_link = f"https://t.me/{bot_user}?start={cid}"

        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(
            InlineKeyboardButton("🔄 Renew Subscription", url=deep_link),
            InlineKeyboardButton("💬 Contact Admin",
                                 url=f"https://t.me/{CONTACT_USERNAME.lstrip('@')}")
        )
        try:
            bot.send_message(
                uid,
                f"⏰ *Subscription Expired*\n\n"
                f"Your subscription to *{ch_name}* has ended and you have been removed.\n\n"
                f"🔄 Renew now to regain access!",
                reply_markup=kb,
                parse_mode="Markdown"
            )
        except Exception as e:
            log.warning("Could not send expiry notice to %d: %s", uid, e)


def _job_warn_expiring():
    """Warn users whose subscription expires in the next 24 hours."""
    now     = datetime.utcnow().timestamp()
    soon    = now + 86400   # 24 hours from now
    expiring = list(users_col.find({
        "expiry":         {"$lte": soon, "$gt": now},
        "warned_expiry":  {"$ne": True}
    }))
    for rec in expiring:
        uid      = rec["user_id"]
        cid      = rec["channel_id"]
        ch_data  = channels_col.find_one({"channel_id": cid})
        ch_name  = ch_data["name"] if ch_data else str(cid)
        bot_user = bot.get_me().username
        link     = f"https://t.me/{bot_user}?start={cid}"
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("🔄 Renew Now", url=link))
        try:
            bot.send_message(
                uid,
                f"⚠️ *Subscription Expiring Soon!*\n\n"
                f"Your subscription to *{ch_name}* expires in less than 24 hours.\n"
                f"📅 Expires: *{fmt_ts(rec['expiry'])}*\n\n"
                f"Renew now to avoid losing access!",
                reply_markup=kb,
                parse_mode="Markdown"
            )
            users_col.update_one(
                {"_id": rec["_id"]},
                {"$set": {"warned_expiry": True}}
            )
        except Exception as e:
            log.warning("Could not send expiry warning to %d: %s", uid, e)


# =============================================================================
# ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    # 1 — Flask keep-alive
    Thread(target=_run_flask, daemon=True).start()
    log.info("🌐 Flask keep-alive started")

    # 2 — Scheduler
    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(_job_kick_expired,    "interval", seconds=60,  id="kicker")
    scheduler.add_job(_job_warn_expiring,   "interval", seconds=3600, id="warner")
    scheduler.start()
    log.info("⏰ Scheduler started (kicker=60s, warner=1h)")

    # 3 — Bot polling
    log.info("🤖 Starting bot polling...")
    try:
        bot.infinity_polling(timeout=30, long_polling_timeout=25)
    except Exception as e:
        log.critical("Polling crashed: %s", e)
    finally:
        scheduler.shutdown()
        log.info("✅ Clean shutdown complete.")
