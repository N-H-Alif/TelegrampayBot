# =============================================================================
#  Telegram Subscription Management Bot  —  v3 ULTIMATE EDITION
#  Fixes: Waiting status card, auto DB cleanup on approve/reject,
#         perfect state machine, duplicate TxID guard, coupon system,
#         referral tracking, revenue stats, /extend, /export, maintenance mode
#  Stack: pyTelegramBotAPI · MongoDB · APScheduler · Flask (Render keep-alive)
# =============================================================================

import os, io, csv, time, logging
from datetime import datetime, timedelta
from threading import Thread

import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from pymongo import MongoClient, ASCENDING, errors as merr
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(name)s │ %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("SubBot")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG  —  all from environment variables
# ─────────────────────────────────────────────────────────────────────────────
BOT_TOKEN        = os.getenv("BOT_TOKEN", "")
MONGO_URI        = os.getenv("MONGO_URI", "")
ADMIN_ID         = int(os.getenv("ADMIN_ID", "0"))
CONTACT_USERNAME = os.getenv("CONTACT_USERNAME", "@admin")

if not BOT_TOKEN or not MONGO_URI or not ADMIN_ID:
    raise SystemExit("❌  Set BOT_TOKEN, MONGO_URI and ADMIN_ID env vars first.")

# ─────────────────────────────────────────────────────────────────────────────
# FLASK KEEP-ALIVE  (Render free tier needs an HTTP port)
# ─────────────────────────────────────────────────────────────────────────────
web = Flask(__name__)

@web.route("/")
def _home():
    return "✅ Sub-Bot v3 is alive!", 200

@web.route("/health")
def _health():
    try:
        db_client.admin.command("ping")
        return jsonify(status="ok", db="connected", ts=datetime.utcnow().isoformat()), 200
    except Exception as e:
        return jsonify(status="error", detail=str(e)), 503

def _run_web():
    web.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), use_reloader=False)

# ─────────────────────────────────────────────────────────────────────────────
# MONGODB  —  exponential-backoff connection
# ─────────────────────────────────────────────────────────────────────────────
def _connect_mongo(uri: str) -> MongoClient:
    delay = 2
    for attempt in range(1, 8):
        try:
            c = MongoClient(uri, serverSelectionTimeoutMS=6000,
                            connectTimeoutMS=6000, socketTimeoutMS=15000)
            c.admin.command("ping")
            log.info("MongoDB connected (attempt %d)", attempt)
            return c
        except merr.ServerSelectionTimeoutError as e:
            log.warning("MongoDB attempt %d failed: %s – retrying in %ds", attempt, e, delay)
            time.sleep(delay); delay = min(delay * 2, 30)
    raise SystemExit("❌  MongoDB unreachable after retries.")

db_client = _connect_mongo(MONGO_URI)
db        = db_client["sub_mgmt_v3"]

# ── Collections ──────────────────────────────────────────────────────────────
channels_col  = db["channels"]   # registered private channels
gateways_col  = db["gateways"]   # payment gateways (multi-currency)
users_col     = db["users"]      # active subscriptions
sessions_col  = db["sessions"]   # payment flow state machine (per user)
adm_col       = db["adm_state"]  # admin conversation state machine
coupons_col   = db["coupons"]    # discount/coupon codes
history_col   = db["history"]    # payment history log (approved payments)
settings_col  = db["settings"]   # bot-wide settings (maintenance mode etc.)
referrals_col = db["referrals"]  # referral tracking

# ── Indexes ──────────────────────────────────────────────────────────────────
users_col.create_index([("expiry", ASCENDING)])
users_col.create_index([("user_id", ASCENDING), ("channel_id", ASCENDING)], unique=True)
sessions_col.create_index([("user_id", ASCENDING)], unique=True)
sessions_col.create_index([("created_at", ASCENDING)], expireAfterSeconds=7200)  # 2-hr TTL
adm_col.create_index([("admin_id", ASCENDING)], unique=True)
coupons_col.create_index([("code", ASCENDING)], unique=True)
history_col.create_index([("user_id", ASCENDING)])
history_col.create_index([("approved_at", ASCENDING)])
referrals_col.create_index([("user_id", ASCENDING)], unique=True)
log.info("All DB indexes ensured.")

# ─────────────────────────────────────────────────────────────────────────────
# BOT
# ─────────────────────────────────────────────────────────────────────────────
bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)

# ─────────────────────────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────────────────────────
def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID

def is_maintenance() -> bool:
    doc = settings_col.find_one({"key": "maintenance"})
    return bool(doc and doc.get("value"))

def fmt_dur(minutes: int) -> str:
    m = int(minutes)
    if m < 60:    return f"{m} Min{'s' if m!=1 else ''}"
    if m < 1440:  h = m//60;   return f"{h} Hour{'s' if h!=1 else ''}"
    if m < 10080: d = m//1440; return f"{d} Day{'s' if d!=1 else ''}"
    if m < 43200: w = m//10080;return f"{w} Week{'s' if w!=1 else ''}"
    mo = m // 43200; return f"{mo} Month{'s' if mo!=1 else ''}"

def fmt_ts(ts: float) -> str:
    return datetime.utcfromtimestamp(ts).strftime("%d %b %Y  %H:%M UTC")

def safe_edit_text(chat_id, msg_id, text, markup=None, md=True):
    """Edit a message, silently ignore 'message not modified' errors."""
    try:
        bot.edit_message_text(text, chat_id, msg_id,
                              reply_markup=markup,
                              parse_mode="Markdown" if md else None)
    except Exception as e:
        if "message is not modified" not in str(e).lower():
            log.warning("edit_message_text error: %s", e)

def safe_edit_caption(chat_id, msg_id, caption, markup=None):
    try:
        bot.edit_message_caption(caption, chat_id, msg_id,
                                 reply_markup=markup, parse_mode="Markdown")
    except Exception as e:
        if "message is not modified" not in str(e).lower():
            log.warning("edit_message_caption error: %s", e)

def send_md(chat_id, text, markup=None):
    return bot.send_message(chat_id, text, parse_mode="Markdown",
                            reply_markup=markup)

def admin_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("📢 Add Channel",    callback_data="adm:add_ch"),
        InlineKeyboardButton("🏦 Add Gateway",    callback_data="adm:add_gw"),
        InlineKeyboardButton("📋 Channels",       callback_data="adm:lst_ch"),
        InlineKeyboardButton("💳 Gateways",       callback_data="adm:lst_gw"),
        InlineKeyboardButton("🔔 Pending Proofs", callback_data="adm:pending"),
        InlineKeyboardButton("📊 Statistics",     callback_data="adm:stats"),
        InlineKeyboardButton("🎟 Coupons",        callback_data="adm:coupons"),
        InlineKeyboardButton("📣 Broadcast",      callback_data="adm:broadcast"),
    )
    return kb

# ─────────────────────────────────────────────────────────────────────────────
# STATE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

# ── User session state ────────────────────────────────────────────────────────
def usr_set(uid: int, step: str, **kw):
    doc = {"user_id": uid, "step": step, "created_at": datetime.utcnow()}
    doc.update(kw)
    sessions_col.update_one({"user_id": uid}, {"$set": doc}, upsert=True)

def usr_get(uid: int) -> dict | None:
    return sessions_col.find_one({"user_id": uid})

def usr_update(uid: int, **kw):
    sessions_col.update_one({"user_id": uid}, {"$set": kw})

def usr_clear(uid: int):
    sessions_col.delete_one({"user_id": uid})

# ── Admin state ───────────────────────────────────────────────────────────────
def adm_set(step: str, **kw):
    doc = {"admin_id": ADMIN_ID, "step": step, "ts": datetime.utcnow()}
    doc.update(kw)
    adm_col.update_one({"admin_id": ADMIN_ID}, {"$set": doc}, upsert=True)

def adm_get() -> dict | None:
    return adm_col.find_one({"admin_id": ADMIN_ID})

def adm_clear():
    adm_col.delete_one({"admin_id": ADMIN_ID})

# ─────────────────────────────────────────────────────────────────────────────
# UNIVERSAL MESSAGE ROUTER  ←  THE CORE FIX (no register_next_step_handler)
# Every incoming message hits this handler. It reads the DB state and routes.
# ─────────────────────────────────────────────────────────────────────────────
@bot.message_handler(
    func=lambda m: True,
    content_types=["text","photo","document","audio","video","voice","sticker","animation"]
)
def router(msg):
    uid  = msg.from_user.id
    text = (msg.text or "").strip()

    # ── Maintenance gate (non-admin users blocked) ────────────────────────────
    if is_maintenance() and not is_admin(uid):
        return send_md(uid,
            "🔧 *Bot is under maintenance.*\n"
            f"Please check back later. Contact {CONTACT_USERNAME}")

    # ── Flood / rate-limit guard (max 1 msg per 1.5 s) ───────────────────────
    sess = usr_get(uid)
    if sess:
        last = sess.get("last_msg_ts", 0)
        now_ts = datetime.utcnow().timestamp()
        if now_ts - last < 1.5 and not is_admin(uid):
            return  # silently drop
    if sess:
        usr_update(uid, last_msg_ts=datetime.utcnow().timestamp())

    # ── /cancel for everyone ──────────────────────────────────────────────────
    if text == "/cancel":
        if is_admin(uid):
            adm_clear()
            send_md(uid, "❌ Action cancelled.", markup=admin_kb())
        else:
            usr_clear(uid)
            send_md(uid, "❌ Payment flow cancelled. Use your subscription link to start again.")
        return

    # ── Admin state machine ───────────────────────────────────────────────────
    if is_admin(uid):
        adm = adm_get()
        if adm:
            step = adm.get("step","")
            dispatch = {
                "ch_fwd":        _adm_ch_fwd,
                "ch_plans":      lambda m: _adm_ch_plans(m, adm),
                "gw_input":      _adm_gw_input,
                "broadcast_msg": _adm_broadcast_msg,
                "coupon_input":  _adm_coupon_input,
                "delch_name":    _adm_delch,
                "delgw_name":    _adm_delgw,
                "kick_input":    _adm_kick_input,
                "extend_input":  _adm_extend_input,
                "reject_reason": lambda m: _adm_reject_reason(m, adm),
            }
            if step in dispatch:
                return dispatch[step](msg)

    # ── User state machine ────────────────────────────────────────────────────
    if sess:
        step = sess.get("step","")
        if step == "await_txid":       return _usr_txid(msg, sess)
        if step == "await_screenshot": return _usr_screenshot(msg, sess)
        if step == "await_coupon":     return _usr_coupon_input(msg, sess)

    # ── Command routing ───────────────────────────────────────────────────────
    if not text.startswith("/"):
        return  # ignore plain text with no active state

    parts = text.split(maxsplit=1)
    cmd   = parts[0].lower().split("@")[0]  # strip @botname suffix
    arg   = parts[1] if len(parts) > 1 else ""

    routes = {
        "/start":       lambda: _cmd_start(msg, arg),
        "/mystatus":    lambda: _cmd_mystatus(msg),
        "/refer":       lambda: _cmd_refer(msg),
        "/couponcheck": lambda: _cmd_couponcheck(msg, arg),
    }
    admin_routes = {
        "/add":          lambda: _adm_prompt_ch(uid),
        "/gateway":      lambda: _adm_prompt_gw(uid),
        "/channels":     lambda: _list_channels(uid),
        "/gateways":     lambda: _list_gateways(uid),
        "/pending":      lambda: _list_pending(uid),
        "/stats":        lambda: _show_stats(uid),
        "/broadcast":    lambda: _adm_prompt_broadcast(uid),
        "/coupon":       lambda: _adm_prompt_coupon(uid),
        "/delchannel":   lambda: _adm_prompt_delch(uid),
        "/delgateway":   lambda: _adm_prompt_delgw(uid),
        "/kick":         lambda: _adm_prompt_kick(uid, arg),
        "/extend":       lambda: _adm_prompt_extend(uid, arg),
        "/history":      lambda: _adm_history(uid, arg),
        "/export":       lambda: _adm_export(uid),
        "/maintenance":  lambda: _adm_maintenance(uid, arg),
    }

    if cmd in routes:
        routes[cmd]()
    elif cmd in admin_routes and is_admin(uid):
        admin_routes[cmd]()
    elif cmd in admin_routes and not is_admin(uid):
        send_md(uid, "❌ You don't have permission to use this command.")

# =============================================================================
#  /start  &  DEEP-LINK ENTRY
# =============================================================================
def _cmd_start(msg, arg: str):
    uid = msg.from_user.id

    # Deep-link: /start 
    if arg:
        try:
            ch_id = int(arg.strip())
        except ValueError:
            return send_md(uid, "❌ Invalid link. Contact " + CONTACT_USERNAME)

        ch = channels_col.find_one({"channel_id": ch_id})
        if not ch:
            return send_md(uid, f"❌ Channel not found.\nContact {CONTACT_USERNAME}")

        # Referral tracking  (if link is "chid_refuid")
        if "_" in arg:
            try:
                parts = arg.split("_")
                ch_id = int(parts[0])
                ref_uid = int(parts[1])
                if ref_uid != uid:
                    referrals_col.update_one(
                        {"user_id": ref_uid},
                        {"$addToSet": {"referred": uid},
                         "$inc": {"count": 1}},
                        upsert=True
                    )
            except Exception:
                pass

        # Already subscribed?
        now = datetime.utcnow().timestamp()
        existing = users_col.find_one({"user_id": uid, "channel_id": ch_id})
        if existing and existing.get("expiry", 0) > now:
            return send_md(uid,
                f"✅ *You already have an active subscription!*\n\n"
                f"📺 Channel: *{ch['name']}*\n"
                f"📅 Expires: `{fmt_ts(existing['expiry'])}`\n\n"
                f"Use /mystatus to see all your subscriptions.")

        plans = ch.get("plans", {})
        if not plans:
            return send_md(uid, "❌ No plans available for this channel yet.")

        kb = InlineKeyboardMarkup(row_width=1)
        for mins_s, price in sorted(plans.items(), key=lambda x: int(x[0])):
            kb.add(InlineKeyboardButton(
                f"⏱ {fmt_dur(int(mins_s))}  ➜  {price}",
                callback_data=f"plan:{ch_id}:{mins_s}"
            ))
        return send_md(uid,
            f"💎 *{ch['name']}*\n\n"
            f"Select your subscription plan 👇",
            markup=kb)

    # Admin panel
    if is_admin(uid):
        return send_md(uid, "🛠 *Admin Control Panel*\nWelcome back!", markup=admin_kb())

    send_md(uid,
        f"👋 *Hello!*\n\n"
        f"To subscribe, please use your channel's subscription link.\n"
        f"Need help? {CONTACT_USERNAME}")

# =============================================================================
#  PLAN SELECTION  →  (optional coupon)  →  currency  →  method  →  details
# =============================================================================
@bot.callback_query_handler(func=lambda c: c.data.startswith("plan:"))
def cb_plan(call):
    uid = call.from_user.id
    if is_maintenance() and not is_admin(uid):
        return bot.answer_callback_query(call.id, "🔧 Bot under maintenance.", show_alert=True)

    _, ch_id_s, mins_s = call.data.split(":", 2)
    ch_id = int(ch_id_s)
    ch = channels_col.find_one({"channel_id": ch_id})
    if not ch:
        return bot.answer_callback_query(call.id, "❌ Channel not found.", show_alert=True)

    # Guard: already has active session?
    sess = usr_get(uid)
    if sess and sess.get("step") in ("await_txid","await_screenshot"):
        return bot.answer_callback_query(call.id,
            "⏳ You have a pending payment. Please wait for admin review!", show_alert=True)

    price    = ch["plans"].get(mins_s, "N/A")
    duration = fmt_dur(int(mins_s))

    # Ask if user has a coupon
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🎟 Apply Coupon", callback_data=f"cpn:{ch_id_s}:{mins_s}"),
        InlineKeyboardButton("⏩ Skip",         callback_data=f"cur:{ch_id_s}:{mins_s}:0"),
    )
    try:
        bot.edit_message_text(
            f"📦 *Plan Selected*\n"
            f"⏱ Duration: *{duration}*\n"
            f"💰 Price:    *{price}*\n\n"
            f"🎟 Do you have a discount coupon?",
            call.message.chat.id, call.message.message_id,
            reply_markup=kb, parse_mode="Markdown")
    except Exception:
        send_md(call.message.chat.id,
            f"📦 *{duration}* — {price}\n\nDo you have a coupon?", markup=kb)
    bot.answer_callback_query(call.id)

# ── Coupon entry from button ──────────────────────────────────────────────────
@bot.callback_query_handler(func=lambda c: c.data.startswith("cpn:"))
def cb_coupon(call):
    uid = call.from_user.id
    _, ch_id_s, mins_s = call.data.split(":", 2)
    usr_set(uid, "await_coupon", channel_id=int(ch_id_s), mins=mins_s)
    bot.answer_callback_query(call.id)
    try: bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception: pass
    send_md(uid,
        "🎟 *Enter Coupon Code*\n\n"
        "Type your coupon code and send it.\n"
        "Send /cancel to skip.")

def _usr_coupon_input(msg, sess):
    uid  = msg.from_user.id
    code = (msg.text or "").strip().upper()
    ch_id_s = str(sess["channel_id"])
    mins_s  = sess["mins"]

    coup = coupons_col.find_one({"code": code})
    if not coup:
        send_md(uid, "❌ Invalid coupon code. Proceeding without discount...")
        usr_clear(uid)
        return _show_currencies(uid, ch_id_s, mins_s, discount=0)

    # Check usage limit and expiry
    if coup.get("used", 0) >= coup.get("limit", 1):
        send_md(uid, "❌ This coupon has expired or reached its usage limit.")
        usr_clear(uid)
        return _show_currencies(uid, ch_id_s, mins_s, discount=0)

    exp = coup.get("expires_at")
    if exp and exp < datetime.utcnow():
        send_md(uid, "❌ This coupon has expired.")
        usr_clear(uid)
        return _show_currencies(uid, ch_id_s, mins_s, discount=0)

    disc = coup.get("discount_pct", 0)
    send_md(uid, f"✅ Coupon *{code}* applied! *{disc}%* discount 🎉")
    usr_clear(uid)
    _show_currencies(uid, ch_id_s, mins_s, discount=disc, coupon_code=code)

# ── Currency selection ────────────────────────────────────────────────────────
@bot.callback_query_handler(func=lambda c: c.data.startswith("cur:"))
def cb_currency(call):
    parts   = call.data.split(":", 3)
    ch_id_s = parts[1]; mins_s = parts[2]; disc_s = parts[3]
    _show_currencies(call.from_user.id, ch_id_s, mins_s,
                     discount=int(disc_s), msg=call.message)
    bot.answer_callback_query(call.id)

def _show_currencies(uid, ch_id_s, mins_s, discount=0, coupon_code="", msg=None):
    currencies = gateways_col.distinct("currency")
    if not currencies:
        return send_md(uid, f"❌ No payment gateways set up.\nContact {CONTACT_USERNAME}")

    flags = {"BDT":"🇧🇩","INR":"🇮🇳","USD":"🇺🇸","USDT":"💵",
             "EUR":"🇪🇺","GBP":"🇬🇧","PKR":"🇵🇰","TRX":"⚡","BTC":"₿"}
    kb = InlineKeyboardMarkup(row_width=2)
    for cur in sorted(currencies):
        kb.add(InlineKeyboardButton(
            f"{flags.get(cur,'💱')} {cur}",
            callback_data=f"meth:{ch_id_s}:{mins_s}:{discount}:{cur}"
        ))

    ch = channels_col.find_one({"channel_id": int(ch_id_s)})
    price = ch["plans"].get(mins_s,"N/A") if ch else "N/A"
    disc_txt = f"\n🎟 Discount: *{discount}%* applied!" if discount else ""

    text = (f"🌍 *Select Currency*\n"
            f"⏱ {fmt_dur(int(mins_s))} — {price}{disc_txt}\n\n"
            f"Choose your payment currency:")
    if msg:
        try:
            bot.edit_message_text(text, msg.chat.id, msg.message_id,
                                  reply_markup=kb, parse_mode="Markdown")
            return
        except Exception: pass
    send_md(uid, text, markup=kb)

# ── Method selection ──────────────────────────────────────────────────────────
@bot.callback_query_handler(func=lambda c: c.data.startswith("meth:"))
def cb_method(call):
    parts   = call.data.split(":", 4)
    ch_id_s = parts[1]; mins_s = parts[2]
    disc    = int(parts[3]); cur = parts[4]

    methods = list(gateways_col.find({"currency": cur}))
    if not methods:
        return bot.answer_callback_query(call.id, "❌ No methods for this currency.", show_alert=True)

    kb = InlineKeyboardMarkup(row_width=1)
    for m in methods:
        kb.add(InlineKeyboardButton(
            f"💳 {m['method_name']}",
            callback_data=f"det:{ch_id_s}:{mins_s}:{disc}:{str(m['_id'])}"
        ))
    kb.add(InlineKeyboardButton("⬅️ Back", callback_data=f"cur:{ch_id_s}:{mins_s}:{disc}"))

    try:
        bot.edit_message_text(
            f"💱 *{cur} Payment Methods*\n\nChoose a method:",
            call.message.chat.id, call.message.message_id,
            reply_markup=kb, parse_mode="Markdown")
    except Exception:
        send_md(call.from_user.id, f"💱 {cur} — Select method:", markup=kb)
    bot.answer_callback_query(call.id)

# ── Payment details ───────────────────────────────────────────────────────────
@bot.callback_query_handler(func=lambda c: c.data.startswith("det:"))
def cb_details(call):
    from bson import ObjectId
    parts   = call.data.split(":", 4)
    ch_id_s = parts[1]; mins_s = parts[2]
    disc    = int(parts[3]); gw_id_s = parts[4]

    try:
        gw = gateways_col.find_one({"_id": ObjectId(gw_id_s)})
    except Exception:
        return bot.answer_callback_query(call.id, "❌ Gateway error.", show_alert=True)
    if not gw:
        return bot.answer_callback_query(call.id, "❌ Gateway removed.", show_alert=True)

    ch = channels_col.find_one({"channel_id": int(ch_id_s)})
    if not ch:
        return bot.answer_callback_query(call.id, "❌ Channel error.", show_alert=True)

    base_price = ch["plans"].get(mins_s, "N/A")
    disc_line  = f"\n🎟 Discount *{disc}%* applied!" if disc else ""
    duration   = fmt_dur(int(mins_s))

    text = (
        f"💳 *Payment Details*\n"
        f"{'━'*28}\n"
        f"📺 Channel:  *{ch['name']}*\n"
        f"⏱ Duration: *{duration}*\n"
        f"💰 Price:    *{base_price}*{disc_line}\n"
        f"{'━'*28}\n"
        f"🏦 Method:   *{gw['method_name']}*\n"
        f"💱 Currency: *{gw['currency']}*\n"
        f"📬 Send to:  `{gw['details']}`\n\n"
        f"📝 *Instructions:*\n{gw['instructions']}\n"
        f"{'━'*28}\n"
        f"✅ After paying, tap *I Have Paid* below."
    )
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("✅ I Have Paid — Submit Proof",
                             callback_data=f"paid:{ch_id_s}:{mins_s}:{disc}:{gw_id_s}"),
        InlineKeyboardButton("⬅️ Back",
                             callback_data=f"meth:{ch_id_s}:{mins_s}:{disc}:{gw['currency']}")
    )
    try:
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id,
                              reply_markup=kb, parse_mode="Markdown")
    except Exception:
        send_md(call.from_user.id, text, markup=kb)
    bot.answer_callback_query(call.id)

# ── "I Have Paid" → begin proof collection (enters state machine) ─────────────
@bot.callback_query_handler(func=lambda c: c.data.startswith("paid:"))
def cb_paid(call):
    from bson import ObjectId
    parts   = call.data.split(":", 4)
    ch_id_s = parts[1]; mins_s = parts[2]
    disc    = int(parts[3]); gw_id_s = parts[4]
    uid     = call.from_user.id

    # Guard: already in a payment flow?
    sess = usr_get(uid)
    if sess and sess.get("step") in ("await_txid","await_screenshot"):
        return bot.answer_callback_query(call.id,
            "⏳ You already have a pending payment! Wait for admin review.", show_alert=True)

    try:
        gw = gateways_col.find_one({"_id": ObjectId(gw_id_s)})
    except Exception:
        return bot.answer_callback_query(call.id, "❌ Gateway error.", show_alert=True)
    ch = channels_col.find_one({"channel_id": int(ch_id_s)})
    if not gw or not ch:
        return bot.answer_callback_query(call.id, "❌ Data error.", show_alert=True)

    # Save session state
    usr_set(uid, "await_txid",
            channel_id   = int(ch_id_s),
            mins         = mins_s,
            gw_id        = gw_id_s,
            method_name  = gw["method_name"],
            currency     = gw["currency"],
            ch_name      = ch["name"],
            price        = ch["plans"].get(mins_s,"N/A"),
            discount     = disc,
            first_name   = call.from_user.first_name or "",
            username     = call.from_user.username or "",
            last_msg_ts  = datetime.utcnow().timestamp())

    bot.answer_callback_query(call.id, "📝 Please send your Transaction ID")

    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception: pass

    send_md(uid,
        "🔐 *Payment Proof — Step 1 of 2*\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Send your *Transaction ID / Reference Number*\n"
        "exactly as shown in your payment app.\n\n"
        "📌 Examples:\n"
        "`TXN9820374651`\n"
        "`BK240601XYZW`\n"
        "`UPI/REF/202406011234`\n\n"
        "⌨️ Type it and send as a plain text message.\n"
        "Send /cancel to abort.")

# =============================================================================
#  STATE HANDLER: await_txid
# =============================================================================
def _usr_txid(msg, sess):
    uid = msg.from_user.id
    if msg.content_type != "text" or not msg.text:
        return send_md(uid,
            "❌ Please send your *Transaction ID as plain text*, "
            "not a photo or file.\nType it and send.")
    txid = msg.text.strip()
    if len(txid) < 4:
        return send_md(uid,
            "❌ That looks too short. "
            "Please check your TxID and resend.")

    # Duplicate TxID guard — check history
    if history_col.find_one({"txid": txid}):
        return send_md(uid,
            "⚠️ *This Transaction ID has already been used.*\n\n"
            "If you believe this is a mistake, contact "
            f"{CONTACT_USERNAME}")

    usr_update(uid, txid=txid, step="await_screenshot",
               last_msg_ts=datetime.utcnow().timestamp())
    send_md(uid,
        f"✅ *TxID saved:* `{txid}`\n\n"
        f"🔐 *Payment Proof — Step 2 of 2*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Now send a *screenshot* of your payment confirmation.\n\n"
        f"📌 Rules:\n"
        f"• Tap 📎 → *Photo / Gallery* → select screenshot\n"
        f"• Do *NOT* send as a File or Document\n"
        f"• Screenshot must clearly show amount & TxID\n\n"
        f"Send /cancel to abort.")

# =============================================================================
#  STATE HANDLER: await_screenshot
# =============================================================================
def _usr_screenshot(msg, sess):
    uid = msg.from_user.id

    # Reject non-photo with helpful hints
    if msg.content_type != "photo" or not msg.photo:
        hint = ""
        if msg.content_type == "document":
            hint = "\n\n⚠️ You sent a *file/document*.\nPlease resend as a *Photo*."
        elif msg.content_type == "text":
            hint = "\n\n⚠️ That's text, not a photo.\nTap 📎 → Photo."
        return send_md(uid,
            f"❌ *Please send a photo screenshot*, "
            f"not a `{msg.content_type}`.{hint}\n\n"
            f"Tap 📎 → choose your screenshot from gallery → send as Photo.")

    photo_fid = msg.photo[-1].file_id  # highest quality frame

    # ── Send "Waiting" status card to user (v3 FIX) ──────────────────────────
    duration  = fmt_dur(int(sess["mins"]))
    disc_line = f"\n🎟 Discount: *{sess.get('discount',0)}%*" if sess.get("discount") else ""
    status_card = send_md(uid,
        f"⏳ *Payment Under Review*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📺 Channel:  *{sess['ch_name']}*\n"
        f"⏱ Duration: *{duration}*\n"
        f"💰 Amount:   *{sess['price']}*{disc_line}\n"
        f"🏦 Method:   *{sess['method_name']}*\n"
        f"🔐 TxID:     `{sess.get('txid','N/A')}`\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🟡 *Status: PENDING REVIEW*\n\n"
        f"Your proof is submitted and awaiting admin review.\n"
        f"You'll be notified here once a decision is made.\n\n"
        f"⏱ Avg review time: *5–30 minutes*\n"
        f"Need help? {CONTACT_USERNAME}")

    # Save screenshot & status_msg_id — screenshot_file_id will be DELETED after decision
    usr_update(uid,
               step               = "submitted",
               screenshot_file_id = photo_fid,
               status_msg_id      = status_card.message_id,
               last_msg_ts        = datetime.utcnow().timestamp())

    # ── Forward proof to admin ───────────────────────────────────────────────
    user_tag = f"@{sess['username']}" if sess.get("username") else sess.get("first_name","?")
    caption  = (
        f"🔔 *New Payment Proof*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 User:     {user_tag} (`{uid}`)\n"
        f"📺 Channel:  *{sess['ch_name']}*\n"
        f"⏱ Duration: *{duration}*\n"
        f"💰 Amount:   *{sess['price']}*{disc_line}\n"
        f"🏦 Method:   *{sess['method_name']}*\n"
        f"💱 Currency: *{sess['currency']}*\n"
        f"🔐 TxID:     `{sess.get('txid','N/A')}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Review screenshot 👆 and decide:"
    )
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Approve", callback_data=f"app:{uid}"),
        InlineKeyboardButton("❌ Reject",  callback_data=f"rej:{uid}"),
    )
    try:
        bot.send_photo(ADMIN_ID, photo=photo_fid, caption=caption,
                       reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        log.error("Could not send proof photo to admin: %s", e)
        send_md(ADMIN_ID,
                caption + f"\n\n📸 FileID: `{photo_fid}`",
                markup=kb)

    log.info("Proof submitted: user=%d channel=%s mins=%s", uid, sess["channel_id"], sess["mins"])

# =============================================================================
#  ADMIN: APPROVE
# =============================================================================
@bot.callback_query_handler(func=lambda c: c.data.startswith("app:"))
def cb_approve(call):
    if not is_admin(call.from_user.id):
        return bot.answer_callback_query(call.id, "❌ Unauthorized")
    uid  = int(call.data.split(":",1)[1])
    sess = sessions_col.find_one({"user_id": uid})
    if not sess:
        bot.answer_callback_query(call.id, "⚠️ Session gone — already processed?", show_alert=True)
        safe_edit_caption(call.message.chat.id, call.message.message_id,
                          call.message.caption + "\n\n⚠️ Already processed")
        return

    ch_id    = sess["channel_id"]
    mins     = int(sess["mins"])
    duration = fmt_dur(mins)
    now      = datetime.utcnow()
    expiry   = now + timedelta(minutes=mins)

    # ── Generate single-use invite link ──────────────────────────────────────
    invite_url = None
    try:
        link_obj   = bot.create_chat_invite_link(
            ch_id, member_limit=1, expire_date=int(expiry.timestamp()))
        invite_url = link_obj.invite_link
    except Exception as e:
        log.error("Invite link error: %s", e)

    # ── Upsert active subscription ────────────────────────────────────────────
    users_col.update_one(
        {"user_id": uid, "channel_id": ch_id},
        {"$set": {
            "user_id":     uid,
            "channel_id":  ch_id,
            "ch_name":     sess.get("ch_name",""),
            "mins":        mins,
            "expiry":      expiry.timestamp(),
            "expiry_dt":   expiry,
            "approved_at": now,
            "method":      sess.get("method_name",""),
            "currency":    sess.get("currency",""),
            "warned_24h":  False,
        }}, upsert=True)

    # ── Write to payment history log ──────────────────────────────────────────
    history_col.insert_one({
        "user_id":     uid,
        "username":    sess.get("username",""),
        "first_name":  sess.get("first_name",""),
        "channel_id":  ch_id,
        "ch_name":     sess.get("ch_name",""),
        "mins":        mins,
        "price":       sess.get("price",""),
        "currency":    sess.get("currency",""),
        "method":      sess.get("method_name",""),
        "txid":        sess.get("txid",""),    # kept in history for records
        "discount":    sess.get("discount",0),
        "approved_at": now,
    })

    # ── Update coupon usage if applicable ────────────────────────────────────
    # (coupon code was stored only in session, already freed)

    # ── Delete session (removes TxID + screenshot_file_id from DB) ───────────
    status_msg_id = sess.get("status_msg_id")
    usr_clear(uid)   # ← THIS is the auto-cleanup: entire session doc deleted

    exp_str = fmt_ts(expiry.timestamp())

    # ── Edit user's "Pending" status card ─────────────────────────────────────
    if status_msg_id:
        try:
            safe_edit_text(uid, status_msg_id,
                f"✅ *Payment Approved!*\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📺 Channel:  *{sess.get('ch_name','')}*\n"
                f"⏱ Duration: *{duration}*\n"
                f"💰 Amount:   *{sess.get('price','N/A')}*\n"
                f"📅 Expires:  `{exp_str}`\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🟢 *Status: APPROVED ✅*")
        except Exception as e:
            log.warning("Could not edit user status card: %s", e)

    # ── Send invite link to user ──────────────────────────────────────────────
    if invite_url:
        ikb = InlineKeyboardMarkup()
        ikb.add(InlineKeyboardButton("🚀 Join Channel Now!", url=invite_url))
        send_md(uid,
            f"🎉 *Access Granted!*\n\n"
            f"Your subscription to *{sess.get('ch_name','')}* is now *ACTIVE*.\n\n"
            f"📅 *Expires:* `{exp_str}`\n\n"
            f"🔗 Your invite link is below:\n"
            f"⚠️ *Single-use only — join immediately!*",
            markup=ikb)
    else:
        send_md(uid,
            f"✅ *Approved!*\nUnable to auto-generate invite link.\n"
            f"Contact {CONTACT_USERNAME} to get access.")

    # ── Edit admin's proof message ────────────────────────────────────────────
    new_cap = (call.message.caption or "") + f"\n\n✅ *APPROVED* — Expires: {exp_str}"
    safe_edit_caption(call.message.chat.id, call.message.message_id, new_cap)
    bot.answer_callback_query(call.id, f"✅ Approved {uid} for {duration}!")
    log.info("Approved: user=%d channel=%d mins=%d", uid, ch_id, mins)

# =============================================================================
#  ADMIN: REJECT  (with optional reason)
# =============================================================================
@bot.callback_query_handler(func=lambda c: c.data.startswith("rej:"))
def cb_reject(call):
    if not is_admin(call.from_user.id):
        return bot.answer_callback_query(call.id, "❌ Unauthorized")
    uid = int(call.data.split(":",1)[1])
    bot.answer_callback_query(call.id)

    # Mark admin as awaiting reject reason
    adm_set("reject_reason", target_uid=uid,
            adm_chat_id  = call.message.chat.id,
            adm_msg_id   = call.message.message_id,
            adm_caption  = call.message.caption or "")
    send_md(ADMIN_ID,
        f"📝 *Rejection Reason*\n\n"
        f"Send the reason to show user `{uid}`.\n"
        f"Or send `skip` to reject without a reason.\n"
        f"Send /cancel to abort.")

def _adm_reject_reason(msg, adm):
    if not msg.text:
        return send_md(ADMIN_ID, "❌ Please send text or `skip`.")
    reason = msg.text.strip()
    uid    = adm["target_uid"]
    adm_clear()

    sess          = sessions_col.find_one({"user_id": uid})
    status_msg_id = sess.get("status_msg_id") if sess else None

    # ── Delete session — removes TxID + screenshot_file_id from DB ───────────
    usr_clear(uid)   # ← auto-cleanup on reject too

    # ── Edit user's status card ───────────────────────────────────────────────
    if status_msg_id:
        try:
            safe_edit_text(uid, status_msg_id,
                f"❌ *Payment Rejected*\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🔴 *Status: REJECTED*\n\n"
                f"Your payment proof was not approved.\n"
                + (f"📋 Reason: _{reason}_\n" if reason != "skip" else "")
                + f"\nPlease try again or contact {CONTACT_USERNAME}")
        except Exception as e:
            log.warning("Could not edit status card on reject: %s", e)

    # ── Notify user ───────────────────────────────────────────────────────────
    ch = channels_col.find_one({"channel_id": sess["channel_id"]}) if sess else None
    bot_user  = bot.get_me().username
    deep_link = f"https://t.me/{bot_user}?start={sess['channel_id']}" if sess else ""
    ikb = InlineKeyboardMarkup(row_width=1)
    if deep_link:
        ikb.add(InlineKeyboardButton("🔄 Try Again", url=deep_link))
    ikb.add(InlineKeyboardButton("💬 Contact Admin",
                                 url=f"https://t.me/{CONTACT_USERNAME.lstrip('@')}"))
    try:
        send_md(uid,
            f"❌ *Payment Rejected*\n\n"
            + (f"📋 Reason: _{reason}_\n\n" if reason != "skip" else "")
            + f"Common issues:\n"
            f"• Incorrect Transaction ID\n"
            f"• Wrong amount sent\n"
            f"• Blurry / wrong screenshot\n\n"
            f"Please try again or contact {CONTACT_USERNAME}.",
            markup=ikb)
    except Exception as e:
        log.error("Could not notify rejected user %d: %s", uid, e)

    # ── Edit admin proof message ──────────────────────────────────────────────
    new_cap = adm["adm_caption"] + "\n\n❌ *REJECTED*" + (
        f" — Reason: _{reason}_" if reason != "skip" else "")
    safe_edit_caption(adm["adm_chat_id"], adm["adm_msg_id"], new_cap)
    send_md(ADMIN_ID, f"❌ User `{uid}` rejected.", markup=admin_kb())
    log.info("Rejected: user=%d", uid)

# =============================================================================
#  ADMIN PANEL CALLBACKS
# =============================================================================
@bot.callback_query_handler(func=lambda c: c.data.startswith("adm:"))
def cb_admin_panel(call):
    if not is_admin(call.from_user.id):
        return bot.answer_callback_query(call.id, "❌ Unauthorized")
    bot.answer_callback_query(call.id)
    action = call.data.split(":",1)[1]
    cid    = call.message.chat.id
    dispatch = {
        "add_ch":    lambda: _adm_prompt_ch(cid),
        "add_gw":    lambda: _adm_prompt_gw(cid),
        "lst_ch":    lambda: _list_channels(cid),
        "lst_gw":    lambda: _list_gateways(cid),
        "pending":   lambda: _list_pending(cid),
        "stats":     lambda: _show_stats(cid),
        "coupons":   lambda: _list_coupons(cid),
        "broadcast": lambda: _adm_prompt_broadcast(cid),
    }
    if action in dispatch: dispatch[action]()

# =============================================================================
#  ADMIN: ADD CHANNEL
# =============================================================================
def _adm_prompt_ch(chat_id):
    adm_set("ch_fwd")
    send_md(chat_id,
        "📢 *Add New Channel*\n\n"
        "Forward any message from your *private channel*.\n\n"
        "⚠️ Bot must be admin there with *Invite Users* permission.\n"
        "Send /cancel to abort.")

def _adm_ch_fwd(msg):
    if not msg.forward_from_chat:
        return send_md(ADMIN_ID,
            "❌ Not a forwarded channel message. "
            "Forward a message *from the channel*.")
    ch_id   = msg.forward_from_chat.id
    ch_name = msg.forward_from_chat.title or f"Channel {ch_id}"
    adm_set("ch_plans", ch_id=ch_id, ch_name=ch_name)
    send_md(ADMIN_ID,
        f"✅ Detected: *{ch_name}*\n\n"
        f"Send subscription plans:\n"
        f"`Minutes:Price, Minutes:Price`\n\n"
        f"📌 Examples:\n"
        f"`1440:100 BDT, 43200:500 BDT, 525600:2000 BDT`\n"
        f"`60:5 USD, 1440:20 USD`\n\n"
        f"Send /cancel to abort.")

def _adm_ch_plans(msg, adm):
    if not msg.text:
        return send_md(ADMIN_ID, "❌ Please send text.")
    try:
        plans = {}
        for entry in msg.text.strip().split(","):
            entry = entry.strip()
            i = entry.index(":")
            mins  = entry[:i].strip()
            price = entry[i+1:].strip()
            if not mins.isdigit(): raise ValueError(f"'{mins}' not a number")
            plans[mins] = price
        if not plans: raise ValueError("Empty")
    except Exception as e:
        return send_md(ADMIN_ID,
            f"❌ Format error: `{e}`\n\nUse: `1440:100 BDT, 43200:500 BDT`")

    ch_id   = adm["ch_id"]; ch_name = adm["ch_name"]
    channels_col.update_one(
        {"channel_id": ch_id},
        {"$set": {"channel_id": ch_id, "name": ch_name,
                  "plans": plans, "updated_at": datetime.utcnow()}},
        upsert=True)
    adm_clear()

    bot_user  = bot.get_me().username
    deep_link = f"https://t.me/{bot_user}?start={ch_id}"
    plans_txt = "\n".join(f"  • {fmt_dur(int(m))} → {p}" for m,p in plans.items())
    send_md(ADMIN_ID,
        f"✅ *Channel Registered!*\n\n"
        f"📢 *{ch_name}*\n\n"
        f"📦 Plans:\n{plans_txt}\n\n"
        f"🔗 Share link:\n`{deep_link}`",
        markup=admin_kb())

# =============================================================================
#  ADMIN: ADD GATEWAY
# =============================================================================
def _adm_prompt_gw(chat_id):
    adm_set("gw_input")
    send_md(chat_id,
        "🏦 *Add Payment Gateway*\n\n"
        "Send: `Currency, MethodName, SendTo, Instructions`\n\n"
        "📌 Examples:\n"
        "`BDT, bKash, 01712345678, Send Money — use number above`\n"
        "`INR, UPI, merchant@ybl, Pay via any UPI app`\n"
        "`USDT, Binance TRC20, TAddr123abc, TRC20 network only`\n\n"
        "Send /cancel to abort.")

def _adm_gw_input(msg):
    if not msg.text:
        return send_md(ADMIN_ID, "❌ Please send text.")
    try:
        parts = [x.strip() for x in msg.text.split(",", 3)]
        if len(parts) != 4: raise ValueError("Need 4 comma-separated fields")
        curr, method, details, instructions = parts
    except Exception as e:
        return send_md(ADMIN_ID,
            f"❌ Error: `{e}`\n\nFormat: `Currency, Method, Details, Instructions`")
    gateways_col.update_one(
        {"method_name": method},
        {"$set": {"currency": curr.upper(), "method_name": method,
                  "details": details, "instructions": instructions,
                  "updated_at": datetime.utcnow()}},
        upsert=True)
    adm_clear()
    send_md(ADMIN_ID,
        f"✅ *Gateway Saved!*\n"
        f"💱 {curr.upper()} — {method}\n"
        f"📬 `{details}`",
        markup=admin_kb())

# =============================================================================
#  ADMIN: LIST CHANNELS / GATEWAYS / PENDING / STATS / COUPONS
# =============================================================================
def _list_channels(chat_id):
    chs = list(channels_col.find())
    if not chs:
        return send_md(chat_id, "📭 No channels registered.")
    bot_user = bot.get_me().username
    lines = ["📋 *Registered Channels*\n"]
    for i,ch in enumerate(chs,1):
        link  = f"https://t.me/{bot_user}?start={ch['channel_id']}"
        plans = "\n".join(f"    • {fmt_dur(int(m))} → {p}"
                          for m,p in ch.get("plans",{}).items())
        lines.append(f"*{i}. {ch['name']}*\n   🆔 `{ch['channel_id']}`"
                     f"\n   🔗 `{link}`\n   📦 Plans:\n{plans}\n")
    send_md(chat_id, "\n".join(lines))

def _list_gateways(chat_id):
    gws = list(gateways_col.find())
    if not gws:
        return send_md(chat_id, "📭 No gateways registered.")
    lines = ["🏦 *Payment Gateways*\n"]
    for i,gw in enumerate(gws,1):
        lines.append(f"*{i}. {gw['method_name']}*\n"
                     f"   💱 {gw['currency']}\n"
                     f"   📬 `{gw['details']}`\n"
                     f"   📝 {gw['instructions']}\n")
    send_md(chat_id, "\n".join(lines))

def _list_pending(chat_id):
    pending = list(sessions_col.find({"step": "submitted"}))
    if not pending:
        return send_md(chat_id, "✅ No pending proofs right now.")
    send_md(chat_id, f"🔔 *{len(pending)} Pending Proof(s)*")
    for sess in pending:
        uid      = sess["user_id"]
        user_tag = f"@{sess['username']}" if sess.get("username") else sess.get("first_name","?")
        duration = fmt_dur(int(sess["mins"]))
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("✅ Approve", callback_data=f"app:{uid}"),
            InlineKeyboardButton("❌ Reject",  callback_data=f"rej:{uid}"),
        )
        text = (f"👤 {user_tag} (`{uid}`)\n"
                f"📺 {sess.get('ch_name','?')}\n"
                f"⏱ {duration} — {sess.get('price','?')}\n"
                f"🏦 {sess.get('method_name','?')}\n"
                f"🔐 TxID: `{sess.get('txid','?')}`")
        if sess.get("screenshot_file_id"):
            try:
                bot.send_photo(chat_id, sess["screenshot_file_id"],
                               caption=text, reply_markup=kb, parse_mode="Markdown")
                continue
            except Exception: pass
        send_md(chat_id, text, markup=kb)

def _show_stats(chat_id):
    now    = datetime.utcnow().timestamp()
    active = users_col.count_documents({"expiry": {"$gt": now}})
    pend   = sessions_col.count_documents({"step": "submitted"})
    total_hist = history_col.count_documents({})
    chs    = channels_col.count_documents({})
    gws    = gateways_col.count_documents({})

    # Per-channel breakdown
    ch_lines = []
    for ch in channels_col.find():
        cnt = users_col.count_documents(
            {"channel_id": ch["channel_id"], "expiry": {"$gt": now}})
        rev_docs = list(history_col.find({"channel_id": ch["channel_id"]}))
        ch_lines.append(f"  • *{ch['name']}*: {cnt} active | {len(rev_docs)} total sales")

    # 30-day revenue count
    since_30 = datetime.utcnow() - timedelta(days=30)
    rev_30   = history_col.count_documents({"approved_at": {"$gte": since_30}})

    send_md(chat_id,
        f"📊 *Bot Statistics*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🟢 Active Subs:       *{active}*\n"
        f"⏳ Pending Proofs:    *{pend}*\n"
        f"📦 Total Sales (all): *{total_hist}*\n"
        f"📅 Sales (30 days):   *{rev_30}*\n"
        f"📢 Channels:          *{chs}*\n"
        f"🏦 Gateways:          *{gws}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"*Per Channel:*\n" + "\n".join(ch_lines) if ch_lines else "  No channels")

def _list_coupons(chat_id):
    coups = list(coupons_col.find())
    if not coups:
        return send_md(chat_id,
            "📭 No coupons created yet.\nUse /coupon to create one.")
    lines = ["🎟 *Active Coupons*\n"]
    for c in coups:
        exp_s = c["expires_at"].strftime("%d %b %Y") if c.get("expires_at") else "Never"
        lines.append(
            f"• Code: `{c['code']}`\n"
            f"  Discount: *{c.get('discount_pct',0)}%*\n"
            f"  Used: *{c.get('used',0)}/{c.get('limit',1)}*\n"
            f"  Expires: {exp_s}\n")
    send_md(chat_id, "\n".join(lines))

# =============================================================================
#  ADMIN: COUPON CREATION
# =============================================================================
def _adm_prompt_coupon(chat_id):
    adm_set("coupon_input")
    send_md(chat_id,
        "🎟 *Create Coupon*\n\n"
        "Send: `CODE, discount_pct, max_uses, days_valid`\n\n"
        "📌 Example:\n"
        "`SAVE20, 20, 100, 30`\n"
        "Creates code SAVE20 → 20% off · 100 uses · valid 30 days\n\n"
        "Send /cancel to abort.")

def _adm_coupon_input(msg):
    if not msg.text:
        return send_md(ADMIN_ID, "❌ Please send text.")
    try:
        parts = [x.strip() for x in msg.text.split(",")]
        if len(parts) != 4: raise ValueError("Need 4 fields")
        code  = parts[0].upper()
        disc  = int(parts[1])
        limit = int(parts[2])
        days  = int(parts[3])
        if not (0 < disc <= 100): raise ValueError("Discount must be 1–100")
        expires = datetime.utcnow() + timedelta(days=days)
    except Exception as e:
        return send_md(ADMIN_ID,
            f"❌ Error: `{e}`\n\nFormat: `CODE, pct, max_uses, days`")
    try:
        coupons_col.insert_one({
            "code": code, "discount_pct": disc,
            "limit": limit, "used": 0, "expires_at": expires,
            "created_at": datetime.utcnow()
        })
    except merr.DuplicateKeyError:
        return send_md(ADMIN_ID, f"❌ Coupon code `{code}` already exists.")
    adm_clear()
    send_md(ADMIN_ID,
        f"✅ *Coupon Created!*\n\n"
        f"🎟 Code: `{code}`\n"
        f"💸 Discount: *{disc}%*\n"
        f"👥 Max uses: *{limit}*\n"
        f"📅 Expires: *{expires.strftime('%d %b %Y')}*",
        markup=admin_kb())

# =============================================================================
#  ADMIN: BROADCAST
# =============================================================================
def _adm_prompt_broadcast(chat_id):
    adm_set("broadcast_msg")
    send_md(chat_id,
        "📣 *Broadcast Message*\n\n"
        "Send the message to broadcast to ALL active subscribers.\n"
        "Supports *Markdown* formatting.\n\n"
        "Send /cancel to abort.")

def _adm_broadcast_msg(msg):
    if not msg.text:
        return send_md(ADMIN_ID, "❌ Please send text.")
    adm_clear()
    now  = datetime.utcnow().timestamp()
    subs = list(users_col.find({"expiry": {"$gt": now}}))
    sent = fails = 0
    for sub in subs:
        try:
            send_md(sub["user_id"],
                    f"📣 *Announcement*\n\n{msg.text}")
            sent += 1
            time.sleep(0.04)
        except Exception: fails += 1
    send_md(ADMIN_ID,
        f"📣 *Broadcast Complete*\n\n"
        f"✅ Sent:   *{sent}*\n"
        f"❌ Failed: *{fails}*",
        markup=admin_kb())

# =============================================================================
#  ADMIN: MANUAL KICK / EXTEND
# =============================================================================
def _adm_prompt_kick(chat_id, arg: str):
    parts = arg.split()
    if len(parts) == 2:
        try:
            uid = int(parts[0]); cid = int(parts[1])
            _do_kick(uid, cid)
            return send_md(chat_id,
                f"✅ Kicked `{uid}` from `{cid}`.", markup=admin_kb())
        except Exception as e:
            return send_md(chat_id, f"❌ Error: `{e}`")
    adm_set("kick_input")
    send_md(chat_id,
        "⚠️ *Manual Kick*\n\n"
        "Send: `user_id channel_id`\n"
        "Example: `123456789 -1001234567890`\n\n"
        "Send /cancel to abort.")

def _adm_kick_input(msg):
    if not msg.text:
        return send_md(ADMIN_ID, "❌ Please send text.")
    try:
        uid, cid = map(int, msg.text.strip().split())
    except Exception:
        return send_md(ADMIN_ID, "❌ Format: `user_id channel_id`")
    adm_clear()
    _do_kick(uid, cid)
    send_md(ADMIN_ID, f"✅ Kicked `{uid}` from `{cid}`.", markup=admin_kb())

def _adm_prompt_extend(chat_id, arg: str):
    parts = arg.split()
    if len(parts) == 3:
        try:
            uid = int(parts[0]); cid = int(parts[1]); mins = int(parts[2])
            _do_extend(uid, cid, mins)
            return send_md(chat_id,
                f"✅ Extended `{uid}` by {fmt_dur(mins)}.", markup=admin_kb())
        except Exception as e:
            return send_md(chat_id, f"❌ Error: `{e}`")
    adm_set("extend_input")
    send_md(chat_id,
        "⏱ *Extend Subscription*\n\n"
        "Send: `user_id channel_id extra_minutes`\n"
        "Example: `123456 -1001234 1440`\n\n"
        "Send /cancel to abort.")

def _adm_extend_input(msg):
    if not msg.text:
        return send_md(ADMIN_ID, "❌ Please send text.")
    try:
        uid, cid, mins = map(int, msg.text.strip().split())
    except Exception:
        return send_md(ADMIN_ID, "❌ Format: `user_id channel_id minutes`")
    adm_clear()
    try:
        _do_extend(uid, cid, mins)
        send_md(ADMIN_ID, f"✅ Extended `{uid}` by {fmt_dur(mins)}.", markup=admin_kb())
    except Exception as e:
        send_md(ADMIN_ID, f"❌ Error: `{e}`")

def _do_kick(uid: int, cid: int):
    try: bot.ban_chat_member(cid, uid)
    except Exception as e: log.warning("Ban error %d/%d: %s", uid, cid, e)
    time.sleep(0.3)
    try: bot.unban_chat_member(cid, uid)
    except Exception as e: log.warning("Unban error %d/%d: %s", uid, cid, e)
    users_col.delete_one({"user_id": uid, "channel_id": cid})

def _do_extend(uid: int, cid: int, extra_mins: int):
    rec = users_col.find_one({"user_id": uid, "channel_id": cid})
    now_ts = datetime.utcnow().timestamp()
    base   = max(rec["expiry"], now_ts) if rec else now_ts
    new_exp = base + extra_mins * 60
    users_col.update_one(
        {"user_id": uid, "channel_id": cid},
        {"$set": {"expiry": new_exp, "warned_24h": False}},
        upsert=True)
    try:
        send_md(uid,
            f"🎁 *Subscription Extended!*\n\n"
            f"Your subscription has been extended by *{fmt_dur(extra_mins)}*.\n"
            f"📅 New expiry: `{fmt_ts(new_exp)}`")
    except Exception: pass

# =============================================================================
#  ADMIN: DELETE CHANNEL / GATEWAY
# =============================================================================
def _adm_prompt_delch(chat_id):
    chs = list(channels_col.find({}, {"name":1}))
    if not chs: return send_md(chat_id, "📭 No channels to delete.")
    adm_set("delch_name")
    names = "\n".join(f"• {c['name']}" for c in chs)
    send_md(chat_id,
        f"🗑 *Delete Channel*\n\nChannels:\n{names}\n\n"
        f"Send exact name to delete.\nSend /cancel to abort.")

def _adm_delch(msg):
    if not msg.text: return send_md(ADMIN_ID, "❌ Please send text.")
    name = msg.text.strip()
    r = channels_col.delete_one({"name": name})
    adm_clear()
    if r.deleted_count:
        send_md(ADMIN_ID, f"✅ Channel *{name}* deleted.", markup=admin_kb())
    else:
        send_md(ADMIN_ID, f"❌ Channel *{name}* not found.", markup=admin_kb())

def _adm_prompt_delgw(chat_id):
    gws = list(gateways_col.find({}, {"method_name":1}))
    if not gws: return send_md(chat_id, "📭 No gateways to delete.")
    adm_set("delgw_name")
    names = "\n".join(f"• {g['method_name']}" for g in gws)
    send_md(chat_id,
        f"🗑 *Delete Gateway*\n\nGateways:\n{names}\n\n"
        f"Send exact method name.\nSend /cancel to abort.")

def _adm_delgw(msg):
    if not msg.text: return send_md(ADMIN_ID, "❌ Please send text.")
    name = msg.text.strip()
    r = gateways_col.delete_one({"method_name": name})
    adm_clear()
    if r.deleted_count:
        send_md(ADMIN_ID, f"✅ Gateway *{name}* deleted.", markup=admin_kb())
    else:
        send_md(ADMIN_ID, f"❌ Gateway *{name}* not found.", markup=admin_kb())

# =============================================================================
#  ADMIN: PAYMENT HISTORY
# =============================================================================
def _adm_history(chat_id, arg: str):
    try:
        uid = int(arg.strip())
    except (ValueError, AttributeError):
        return send_md(chat_id,
            "Usage: `/history user_id`\nExample: `/history 123456789`")
    records = list(history_col.find({"user_id": uid})
                               .sort("approved_at", -1).limit(10))
    if not records:
        return send_md(chat_id, f"📭 No payment history for `{uid}`.")
    lines = [f"📜 *Payment History — User `{uid}`*\n"]
    for i, r in enumerate(records, 1):
        date = r["approved_at"].strftime("%d %b %Y %H:%M") if r.get("approved_at") else "?"
        lines.append(
            f"*{i}.* {date}\n"
            f"   📺 {r.get('ch_name','?')} — {fmt_dur(r.get('mins',0))}\n"
            f"   💰 {r.get('price','?')} via {r.get('method','?')}\n"
            f"   🔐 TxID: `{r.get('txid','?')}`\n")
    send_md(chat_id, "\n".join(lines))

# =============================================================================
#  ADMIN: EXPORT CSV
# =============================================================================
def _adm_export(chat_id):
    now  = datetime.utcnow().timestamp()
    subs = list(users_col.find({"expiry": {"$gt": now}}))
    if not subs:
        return send_md(chat_id, "📭 No active subscriptions to export.")

    buf = io.StringIO()
    w   = csv.writer(buf)
    w.writerow(["user_id","channel_id","ch_name","mins","expiry","method","currency"])
    for s in subs:
        w.writerow([
            s.get("user_id",""), s.get("channel_id",""),
            s.get("ch_name",""), s.get("mins",""),
            fmt_ts(s.get("expiry",0)),
            s.get("method",""), s.get("currency",""),
        ])
    buf.seek(0)
    bot.send_document(
        chat_id,
        ("active_subs.csv", buf.getvalue().encode("utf-8"), "text/csv"),
        caption=f"📊 Active subscriptions export — {len(subs)} records")

# =============================================================================
#  ADMIN: MAINTENANCE MODE
# =============================================================================
def _adm_maintenance(chat_id, arg: str):
    a = arg.strip().lower()
    if a == "on":
        settings_col.update_one(
            {"key": "maintenance"}, {"$set": {"value": True}}, upsert=True)
        send_md(chat_id, "🔧 *Maintenance mode ON.*\n"
                "All non-admin users are blocked.", markup=admin_kb())
    elif a == "off":
        settings_col.update_one(
            {"key": "maintenance"}, {"$set": {"value": False}}, upsert=True)
        send_md(chat_id, "✅ *Maintenance mode OFF.*\n"
                "Bot is now public.", markup=admin_kb())
    else:
        on = is_maintenance()
        send_md(chat_id,
            f"🔧 Maintenance is currently: *{'ON' if on else 'OFF'}*\n\n"
            f"Use `/maintenance on` or `/maintenance off`")

# =============================================================================
#  USER: /mystatus
# =============================================================================
def _cmd_mystatus(msg):
    uid  = msg.from_user.id
    now  = datetime.utcnow().timestamp()
    subs = list(users_col.find({"user_id": uid, "expiry": {"$gt": now}}))
    sess = sessions_col.find_one({"user_id": uid})

    if not subs and not sess:
        return send_md(uid,
            "📭 *No Active Subscriptions*\n\n"
            "You have no active subscriptions.\n"
            f"Contact {CONTACT_USERNAME} for help.")

    lines = ["📋 *Your Subscriptions*\n"]
    for s in subs:
        rem_s  = s["expiry"] - now
        rem_h  = rem_s / 3600
        if rem_h < 24:
            rem = f"⚠️ {rem_h:.1f}h remaining"
        else:
            rem = f"✅ {rem_h/24:.1f} days remaining"
        lines.append(
            f"📺 *{s.get('ch_name','Channel')}*\n"
            f"   📅 Expires: `{fmt_ts(s['expiry'])}`\n"
            f"   ⏳ {rem}\n")

    if sess and sess.get("step") in ("await_txid","await_screenshot","submitted"):
        lines.append(
            f"\n⏳ *Pending Payment*\n"
            f"   Channel: {sess.get('ch_name','?')}\n"
            f"   Status: {sess.get('step','?').replace('_',' ').title()}")
    send_md(uid, "\n".join(lines))

# =============================================================================
#  USER: /refer  — referral system
# =============================================================================
def _cmd_refer(msg):
    uid      = msg.from_user.id
    bot_user = bot.get_me().username
    ref_doc  = referrals_col.find_one({"user_id": uid}) or {}
    count    = ref_doc.get("count", 0)

    # For first channel (or let user pick — simplified to first registered channel)
    ch = channels_col.find_one()
    if not ch:
        return send_md(uid, "❌ No channels set up yet.")

    ref_link = f"https://t.me/{bot_user}?start={ch['channel_id']}_{uid}"
    send_md(uid,
        f"🔗 *Your Referral Link*\n\n"
        f"`{ref_link}`\n\n"
        f"Share this link! When someone subscribes using your link:\n"
        f"• They get signed up\n"
        f"• You earn referral credit\n\n"
        f"👥 *Your referrals so far: {count}*\n\n"
        f"Contact {CONTACT_USERNAME} to redeem referral rewards!")

# =============================================================================
#  USER: /couponcheck
# =============================================================================
def _cmd_couponcheck(msg, arg: str):
    code = arg.strip().upper() if arg else ""
    if not code:
        return send_md(msg.from_user.id,
            "Usage: `/couponcheck CODENAME`\nExample: `/couponcheck SAVE20`")
    coup = coupons_col.find_one({"code": code})
    if not coup:
        return send_md(msg.from_user.id, f"❌ Coupon `{code}` not found.")
    exp   = coup.get("expires_at")
    valid = not (exp and exp < datetime.utcnow())
    used  = coup.get("used",0); limit = coup.get("limit",1)
    avail = valid and used < limit
    send_md(msg.from_user.id,
        f"🎟 *Coupon: {code}*\n\n"
        f"💸 Discount: *{coup.get('discount_pct',0)}%*\n"
        f"👥 Used: *{used}/{limit}*\n"
        f"📅 Expires: {exp.strftime('%d %b %Y') if exp else 'Never'}\n"
        f"{'✅ Valid and available!' if avail else '❌ Not available'}")

# =============================================================================
#  SCHEDULER JOBS
# =============================================================================
def _job_kick_expired():
    """Runs every 60 s — kicks expired subscribers and sends renewal notice."""
    now     = datetime.utcnow().timestamp()
    expired = list(users_col.find({"expiry": {"$lte": now}}))
    if not expired: return
    log.info("Kicker: %d expired", len(expired))
    bot_user = bot.get_me().username

    for rec in expired:
        uid = rec["user_id"]; cid = rec["channel_id"]
        _do_kick(uid, cid)

        ch        = channels_col.find_one({"channel_id": cid})
        ch_name   = ch["name"] if ch else str(cid)
        deep_link = f"https://t.me/{bot_user}?start={cid}"
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(InlineKeyboardButton("🔄 Renew Subscription", url=deep_link))
        kb.add(InlineKeyboardButton("💬 Contact Admin",
               url=f"https://t.me/{CONTACT_USERNAME.lstrip('@')}"))
        try:
            send_md(uid,
                f"⏰ *Subscription Expired*\n\n"
                f"Your subscription to *{ch_name}* has ended.\n"
                f"You have been removed from the channel.\n\n"
                f"🔄 Tap below to renew and regain access!",
                markup=kb)
        except Exception as e:
            log.warning("Could not send expiry notice to %d: %s", uid, e)

def _job_warn_expiring():
    """Runs every hour — warns users expiring within 24 h."""
    now  = datetime.utcnow().timestamp()
    soon = now + 86400
    for rec in users_col.find({"expiry": {"$lte": soon, "$gt": now},
                                "warned_24h": {"$ne": True}}):
        uid     = rec["user_id"]; cid = rec["channel_id"]
        ch      = channels_col.find_one({"channel_id": cid})
        ch_name = ch["name"] if ch else str(cid)
        bot_u   = bot.get_me().username
        link    = f"https://t.me/{bot_u}?start={cid}"
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("🔄 Renew Now", url=link))
        try:
            send_md(uid,
                f"⚠️ *Subscription Expiring Soon!*\n\n"
                f"*{ch_name}* expires in less than 24 hours.\n"
                f"📅 Expires: `{fmt_ts(rec['expiry'])}`\n\n"
                f"Renew now to stay connected!",
                markup=kb)
            users_col.update_one({"_id": rec["_id"]}, {"$set": {"warned_24h": True}})
        except Exception as e:
            log.warning("Could not warn user %d: %s", uid, e)

def _job_cleanup_stale_sessions():
    """Runs every 30 min — removes sessions stuck in non-submitted state for >2 h."""
    cutoff = datetime.utcnow() - timedelta(hours=2)
    r = sessions_col.delete_many({
        "step":       {"$in": ["await_txid","await_screenshot"]},
        "created_at": {"$lt": cutoff}
    })
    if r.deleted_count:
        log.info("Cleaned %d stale sessions", r.deleted_count)

# =============================================================================
#  ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    Thread(target=_run_web, daemon=True).start()
    log.info("Flask keep-alive started")

    sched = BackgroundScheduler(timezone="UTC")
    sched.add_job(_job_kick_expired,        "interval", seconds=60,   id="kicker")
    sched.add_job(_job_warn_expiring,       "interval", seconds=3600, id="warner")
    sched.add_job(_job_cleanup_stale_sessions, "interval", seconds=1800, id="cleaner")
    sched.start()
    log.info("Scheduler started")

    log.info("Bot polling started — v3 Ultimate Edition")
    try:
        bot.infinity_polling(timeout=30, long_polling_timeout=25)
    except Exception as e:
        log.critical("Polling crashed: %s", e)
    finally:
        sched.shutdown()
        log.info("Clean shutdown.")
