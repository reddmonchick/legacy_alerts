"""
Парсер экономического календаря Forex Factory + публикация в Discord.

Источник: официальный (хоть и "неофициальный") JSON-фид,
который сама Forex Factory отдаёт для EA/индикаторов MT4/MT5.
Время в фиде — America/New_York (Eastern Time).

- В канале news больше НЕ постится никаких новостей — там только панель
  настроек (выбор валют и тайминга уведомлений).
- Через панель в канале news пользователь выбирает валюты и тайминг(и)
  уведомлений (5/15/30/60 мин). Бот создаёт ему персональный тред внутри
  канала news и пишет туда только события по выбранным валютам, в выбранное
  время. Подписки хранятся в PostgreSQL.

pip install requests python-dateutil pytz discord.py python-decouple asyncpg
"""

import asyncio
import logging
import os
import threading
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests
import pytz
import discord
import asyncpg
from discord.ext import commands, tasks
from dateutil import parser as date_parser
from decouple import config

# ──────────────────────────────────────────────────────────────────
# Конфиг
# ──────────────────────────────────────────────────────────────────

FEED_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
FEED_URL_MIRROR = "https://cdn-nfs.faireconomy.media/ff_calendar_thisweek.json"

SOURCE_TZ = pytz.timezone("America/New_York")
TARGET_TZ_NAME = config("CALENDAR_TZ", default="Europe/Oslo")
TARGET_TZ = pytz.timezone(TARGET_TZ_NAME)

DISCORD_TOKEN = config("DISCORD_BOT_TOKEN")
NEWS_CHANNEL_ID = config("NEWS_CHANNEL_ID", default=None, cast=lambda v: int(v) if v else None)
NEWS_CHANNEL_NAME = config("NEWS_CHANNEL_NAME", default="news")
GUILD_ID = config("GUILD_ID", default=None, cast=lambda v: int(v) if v else None)

# ID пользователей, которым разрешено публиковать панель /post_filter_panel.
# Через запятую в .env, например: ADMIN_USER_IDS=111111111111111111,222222222222222222
ADMIN_USER_IDS = config(
    "ADMIN_USER_IDS",
    default="",
    cast=lambda v: {int(x) for x in v.split(",") if x.strip()},
)

# postgresql://user:password@host:5432/dbname
DATABASE_URL = config("DATABASE_URL")

# Публичный URL сервиса (напр. https://xxx.onrender.com). Если задан,
# бот сам пингует его каждые 10 минут, чтобы бесплатный инстанс Render
# не уснул по бездействию (не даём ему "входящий трафик").
SELF_PING_URL = config("SELF_PING_URL", default=None)

MIN_IMPACT = "High"
CHECK_INTERVAL_SECONDS = 60
REFRESH_CALENDAR_MINUTES = 60
THREAD_AUTO_ARCHIVE_MINUTES = 10080         # неделя — максимум, который разрешает Discord

CURRENCY_OPTIONS = ["USD", "EUR", "GBP", "JPY", "AUD", "NZD", "CAD", "CHF", "CNY"]
CURRENCY_FLAG = {
    "USD": "🇺🇸",
    "EUR": "🇪🇺",
    "GBP": "🇬🇧",
    "JPY": "🇯🇵",
    "AUD": "🇦🇺",
    "NZD": "🇳🇿",
    "CAD": "🇨🇦",
    "CHF": "🇨🇭",
    "CNY": "🇨🇳",
}
LEAD_TIME_OPTIONS_MINUTES = [5, 15, 30, 60]  # варианты тайминга персональных уведомлений

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ff_calendar_bot")


# ──────────────────────────────────────────────────────────────────
# Парсер календаря
# ──────────────────────────────────────────────────────────────────

def fetch_calendar(target_tz_name: str = TARGET_TZ_NAME):
    try:
        resp = requests.get(FEED_URL, headers=HEADERS, timeout=10)
        resp.raise_for_status()
    except requests.RequestException:
        resp = requests.get(FEED_URL_MIRROR, headers=HEADERS, timeout=10)
        resp.raise_for_status()

    raw_events = resp.json()
    target_tz = pytz.timezone(target_tz_name)

    events = []
    for e in raw_events:
        # пример поля e['date']: "2026-07-29T14:00:00-04:00" (ISO 8601 со смещением ET)
        dt = date_parser.parse(e["date"])
        dt = SOURCE_TZ.localize(dt) if dt.tzinfo is None else dt
        dt_local = dt.astimezone(target_tz)

        events.append({
            "title": e.get("title"),
            "country": e.get("country"),
            "impact": e.get("impact"),
            "forecast": e.get("forecast"),
            "previous": e.get("previous"),
            "datetime": dt_local,
        })

    return events


def events_for_day(events, target_date, min_impact="High"):
    impact_order = {"Low": 0, "Medium": 1, "High": 2, "Holiday": -1}
    threshold = impact_order.get(min_impact, 2)

    result = [
        e for e in events
        if e["datetime"].date() == target_date
        and impact_order.get(e["impact"], -1) >= threshold
    ]
    result.sort(key=lambda e: e["datetime"])
    return result


def _event_key(e: dict) -> tuple:
    return (e.get("title"), e.get("country"), e["datetime"].isoformat())


def _make_embed(e: dict) -> discord.Embed:
    minutes_left = int((e["datetime"] - datetime.now(TARGET_TZ)).total_seconds() // 60)
    embed = discord.Embed(
        title=f"🔴 {e['title']}",
        description=f"Через ~{minutes_left} мин.",
        color=discord.Color.red(),
        timestamp=e["datetime"],
    )
    embed.add_field(name="Страна", value=e.get("country") or "—", inline=True)
    embed.add_field(name="Impact", value=e.get("impact") or "—", inline=True)
    embed.add_field(name="Время", value=e["datetime"].strftime("%H:%M %Z"), inline=True)
    embed.add_field(name="Прогноз", value=e.get("forecast") or "—", inline=True)
    embed.add_field(name="Предыдущее", value=e.get("previous") or "—", inline=True)
    return embed


# ──────────────────────────────────────────────────────────────────
# Слой доступа к PostgreSQL
# ──────────────────────────────────────────────────────────────────

_db_pool: asyncpg.Pool | None = None


async def _init_db() -> None:
    global _db_pool
    _db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with _db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                user_id      BIGINT PRIMARY KEY,
                currencies   TEXT[] NOT NULL DEFAULT '{}',
                thread_id    BIGINT,
                lead_minutes INTEGER[] NOT NULL DEFAULT '{60}'
            )
        """)
        # на случай, если таблица уже существовала со старой версии бота (без этих колонок)
        await conn.execute("ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS thread_id BIGINT")
        await conn.execute(
            "ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS lead_minutes INTEGER[] NOT NULL DEFAULT '{60}'"
        )
    log.info("Подключение к PostgreSQL установлено, таблица subscriptions готова")


async def _get_subscription(user_id: int) -> dict:
    async with _db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT currencies, thread_id, lead_minutes FROM subscriptions WHERE user_id = $1", user_id
        )
    if row is None:
        return {"currencies": [], "thread_id": None, "lead_minutes": [60]}
    return {
        "currencies": list(row["currencies"]),
        "thread_id": row["thread_id"],
        "lead_minutes": list(row["lead_minutes"]),
    }


async def _set_currencies(user_id: int, currencies: list[str]) -> None:
    async with _db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO subscriptions (user_id, currencies) VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET currencies = EXCLUDED.currencies
        """, user_id, currencies)


async def _set_lead_minutes(user_id: int, lead_minutes: list[int]) -> None:
    async with _db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO subscriptions (user_id, lead_minutes) VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET lead_minutes = EXCLUDED.lead_minutes
        """, user_id, lead_minutes)


async def _set_thread_id(user_id: int, thread_id: int) -> None:
    async with _db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO subscriptions (user_id, thread_id) VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET thread_id = EXCLUDED.thread_id
        """, user_id, thread_id)


async def _clear_thread_id(user_id: int) -> None:
    async with _db_pool.acquire() as conn:
        await conn.execute("UPDATE subscriptions SET thread_id = NULL WHERE user_id = $1", user_id)


async def _delete_subscription(user_id: int) -> None:
    async with _db_pool.acquire() as conn:
        await conn.execute("DELETE FROM subscriptions WHERE user_id = $1", user_id)


async def _all_subscriptions() -> list[dict]:
    async with _db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id, currencies, thread_id, lead_minutes FROM subscriptions")
    return [
        {
            "user_id": r["user_id"],
            "currencies": list(r["currencies"]),
            "thread_id": r["thread_id"],
            "lead_minutes": list(r["lead_minutes"]),
        }
        for r in rows
    ]


# ──────────────────────────────────────────────────────────────────
# Discord-бот
# ──────────────────────────────────────────────────────────────────

intents = discord.Intents.default()


class CalendarBot(commands.Bot):
    async def setup_hook(self):
        await _init_db()
        # регистрируем persistent view заново после каждого рестарта,
        # чтобы старое закреплённое сообщение с меню продолжало работать
        self.add_view(FilterPanelView())
        self.add_view(PersonalMenuView())


bot = CalendarBot(command_prefix="!", intents=intents)

_events_cache: list[dict] = []
_posted_user_keys: set[tuple] = set()  # (user_id, event_key, lead_minutes) — дедуп персональных увед.
_news_channel: discord.TextChannel | None = None


async def _resolve_news_channel() -> discord.TextChannel | None:
    if NEWS_CHANNEL_ID:
        ch = bot.get_channel(int(NEWS_CHANNEL_ID))
        if ch is not None:
            return ch
        log.warning("NEWS_CHANNEL_ID=%s указан, но канал не найден в кэше клиента", NEWS_CHANNEL_ID)

    for guild in bot.guilds:
        for ch in guild.text_channels:
            if ch.name == NEWS_CHANNEL_NAME:
                return ch
    return None


async def _delete_thread_created_notice() -> None:
    """
    Discord сам постит в родительский канал системное сообщение
    "X начал(а) ветку: <название>" при создании треда. Удаляем его,
    чтобы не засорять канал news. Требует право Manage Messages.
    """
    if _news_channel is None:
        return
    try:
        async for msg in _news_channel.history(limit=5):
            if msg.type == discord.MessageType.thread_created:
                await msg.delete()
                return
    except discord.Forbidden:
        log.warning("Нет права Manage Messages в news — не могу удалить системное сообщение о треде")
    except discord.HTTPException:
        log.exception("Ошибка при удалении системного сообщения о создании треда")


async def _get_or_create_user_thread(user: discord.abc.User) -> discord.Thread | None:
    """Возвращает существующий тред пользователя в канале news или создаёт новый."""
    global _news_channel
    if _news_channel is None:
        _news_channel = await _resolve_news_channel()
    if _news_channel is None:
        return None

    record = await _get_subscription(user.id)
    thread_id = record.get("thread_id")

    thread = None
    if thread_id:
        thread = bot.get_channel(thread_id)
        if thread is None:
            try:
                thread = await bot.fetch_channel(thread_id)
            except (discord.NotFound, discord.Forbidden):
                thread = None

    if thread is None:
      thread_name = f"news-{user.name}"[:100]
      try:
          thread = await _news_channel.create_thread(
              name=thread_name,
              type=discord.ChannelType.public_thread,
              auto_archive_duration=THREAD_AUTO_ARCHIVE_MINUTES,
          )
      except discord.HTTPException:
          log.exception("Не удалось создать тред для %s", user)
          return None
  
      await _set_thread_id(user.id, thread.id)   # сохраняем СРАЗУ, до add_user
      await _delete_thread_created_notice()
  
      try:
          await thread.add_user(user)
      except discord.HTTPException:
          log.warning("Не удалось добавить %s в тред, но тред сохранён", user)
        
    elif getattr(thread, "archived", False):
        try:
            await thread.edit(archived=False)
        except discord.HTTPException:
            pass

    return thread


async def _notify_subscribers(e: dict, now: datetime) -> None:
    """
    Пишет в персональный тред каждому, кто подписан на валюту этого события,
    учитывая ИНДИВИДУАЛЬНЫЙ список таймингов пользователя (может быть
    несколько: например и за 30, и за 5 минут до новости).
    """
    country = e.get("country")
    if not country:
        return

    event_key = _event_key(e)
    delta = e["datetime"] - now

    for record in await _all_subscriptions():
        if country not in record["currencies"]:
            continue

        thread_id = record["thread_id"]
        if not thread_id:
            continue

        for lead_minutes in record["lead_minutes"]:
            lead_td = timedelta(minutes=lead_minutes)
            # окно чуть шире интервала проверки, чтобы не пропустить событие
            # из-за дрейфа таймера; повторные посты исключает дедуп
            window_start = lead_td - timedelta(seconds=2 * CHECK_INTERVAL_SECONDS)
            if not (window_start <= delta <= lead_td):
                continue

            user_key = (record["user_id"], event_key, lead_minutes)
            if user_key in _posted_user_keys:
                continue

            try:
                thread = bot.get_channel(thread_id) or await bot.fetch_channel(thread_id)
                if getattr(thread, "archived", False):
                    await thread.edit(archived=False)
                await thread.send(embed=_make_embed(e))
                _posted_user_keys.add(user_key)
            except (discord.NotFound, discord.Forbidden):
                log.warning("Тред пользователя %s недоступен — сбрасываю привязку", record["user_id"])
                await _clear_thread_id(record["user_id"])
            except discord.HTTPException:
                log.exception("Ошибка отправки в тред пользователя %s", record["user_id"])

    if len(_posted_user_keys) > 2000:
        _posted_user_keys.clear()


# ── Панель выбора валют ──

async def _subscription_embed(user_id: int) -> discord.Embed:
    """Карточка подписки пользователя (для личного меню)."""
    record = await _get_subscription(user_id)
    currencies_text = ", ".join(record["currencies"]) if record["currencies"] else "не выбраны"
    lead_text = ", ".join(f"{m} мин" for m in sorted(record["lead_minutes"])) or "не выбрано"
    embed = discord.Embed(
        title="📊 Ваша подписка",
        description=f"Валюты: **{currencies_text}**\nУведомления: за **{lead_text}**\n\nНовости будут приходить в вашу личную ветку.",
        color=discord.Color.green(),
    )
    embed.set_footer(text="Выбор сохраняется автоматически")
    return embed


class CurrencyFilterSelect(discord.ui.Select):
    """
    custom_id фиксированный (не привязан к пользователю) — это то,
    что делает view persistent: после рестарта бот регистрирует
    её заново в setup_hook, и старое сообщение продолжает работать.
    """
    def __init__(self, defaults: list[str] | None = None):
        defaults = defaults or []
        options = [
            discord.SelectOption(label=cur, value=cur, emoji=CURRENCY_FLAG[cur], default=cur in defaults)
            for cur in CURRENCY_OPTIONS
        ]
        super().__init__(
            placeholder="Выберите валюты, которые вам важны...",
            min_values=0,
            max_values=len(options),
            options=options,
            custom_id="ff_currency_filter_select",
        )

    async def callback(self, interaction: discord.Interaction):
        user = interaction.user

        if self.values:
            await _set_currencies(user.id, list(self.values))
            await _get_or_create_user_thread(user)
        else:
            # Не удаляем строку целиком, а только очищаем валюты —
            # иначе теряется thread_id и при повторном выборе создаётся новый тред.
            await _set_currencies(user.id, [])

        embed = await _subscription_embed(user.id)
        view = await PersonalMenuView.for_user(user.id)
        await interaction.response.edit_message(embed=embed, view=view)


class LeadTimeSelect(discord.ui.Select):
    """Выбор одного или нескольких таймингов для персональных уведомлений."""
    def __init__(self, defaults: list[str] | None = None):
        defaults = defaults or []
        options = [
            discord.SelectOption(label=f"За {m} мин.", value=str(m), default=str(m) in defaults)
            for m in LEAD_TIME_OPTIONS_MINUTES
        ]
        super().__init__(
            placeholder="За сколько минут присылать уведомление (можно несколько)...",
            min_values=0,
            max_values=len(options),
            options=options,
            custom_id="ff_lead_time_select",
        )

    async def callback(self, interaction: discord.Interaction):
        lead_values = sorted(int(v) for v in self.values) if self.values else []
        await _set_lead_minutes(interaction.user.id, lead_values)
        embed = await _subscription_embed(interaction.user.id)
        view = await PersonalMenuView.for_user(interaction.user.id)
        await interaction.response.edit_message(embed=embed, view=view)


class PersonalMenuView(discord.ui.View):
    """Личное меню подписки — отправляется только нажавшему (ephemeral)."""
    def __init__(self, currency_defaults: list[str] | None = None, lead_defaults: list[str] | None = None):
        super().__init__(timeout=None)  # timeout=None обязателен для persistent view
        self.add_item(CurrencyFilterSelect(currency_defaults))
        self.add_item(LeadTimeSelect(lead_defaults))

    @classmethod
    async def for_user(cls, user_id: int) -> "PersonalMenuView":
        """Собирает меню с уже отмеченными текущими выборами пользователя."""
        record = await _get_subscription(user_id)
        return cls(record["currencies"], [str(m) for m in record["lead_minutes"]])


class SubscribeButton(discord.ui.Button):
    """Кнопка в общей панели: открывает личное меню подписки для нажавшего."""
    def __init__(self):
        super().__init__(
            label="⚙️ Настроить подписку",
            style=discord.ButtonStyle.primary,
            custom_id="ff_subscribe_button",
        )

    async def callback(self, interaction: discord.Interaction):
        embed = await _subscription_embed(interaction.user.id)
        view = await PersonalMenuView.for_user(interaction.user.id)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


class FilterPanelView(discord.ui.View):
    """Общая панель в канале — только кнопка, личного в ней ничего нет."""
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(SubscribeButton())


def _panel_embed() -> discord.Embed:
    """Карточка общей панели в канале news."""
    return discord.Embed(
        title="📊 Экономический календарь",
        description=(
            "⚠️ С первого раза может появиться ошибка — просто повторите действие.\n\n"
            "Хотите получать уведомления о важных (High Impact) новостях по выбранным валютам? "
            "Нажмите кнопку ниже — настройки откроются лично для вас."
        ),
        color=discord.Color.blurple(),
    )


async def _refresh_panel_on_startup() -> None:
    """
    При старте бота проверяет закреплённую панель в канале news:
    если это старая версия карточки (без предупреждения про ошибку) —
    удаляет её и публикует новую, чтобы обновление текста применялось
    без ручного перезапуска команды /post_filter_panel.
    """
    global _news_channel
    if _news_channel is None:
        _news_channel = await _resolve_news_channel()
    if _news_channel is None:
        log.warning("Канал news не найден — пропускаю обновление панели")
        return

    HINT = "С первого раза может появиться ошибка"
    try:
        pinned = await _news_channel.pins()
    except discord.HTTPException:
        log.exception("Не удалось получить закреплённые сообщения в news")
        return

    for msg in pinned:
        if msg.author != bot.user or not msg.embeds:
            continue
        embed = msg.embeds[0]
        if embed.title != "📊 Экономический календарь":
            continue
        if HINT in (embed.description or ""):
            return
        try:
            await msg.delete()
            new_msg = await _news_channel.send(embed=_panel_embed(), view=FilterPanelView())
            try:
                await new_msg.pin()
            except discord.HTTPException:
                log.warning("Не удалось закрепить обновлённую панель — закрепите её вручную")
            log.info("Панель в news обновлена на новую версию")
        except discord.HTTPException:
            log.exception("Не удалось заменить панель в news")
        return


@bot.tree.command(
    name="post_filter_panel",
    description="Опубликовать в канале news панель выбора валют для подписки",
)
async def post_filter_panel_cmd(interaction: discord.Interaction):
    if interaction.user.id not in ADMIN_USER_IDS:
        await interaction.response.send_message(
            "Эта команда доступна только администраторам бота.", ephemeral=True
        )
        return

    global _news_channel
    if _news_channel is None:
        _news_channel = await _resolve_news_channel()
    if _news_channel is None:
        await interaction.response.send_message("Канал news не найден.", ephemeral=True)
        return

    embed = _panel_embed()
    msg = await _news_channel.send(embed=embed, view=FilterPanelView())
    try:
        await msg.pin()
    except discord.HTTPException:
        log.warning("Не удалось закрепить панель — сделайте это вручную")

    await interaction.response.send_message("Панель опубликована и закреплена.", ephemeral=True)


@bot.tree.command(
    name="clean_news_channel",
    description="Удалить старые сообщения бота в канале news, кроме закреплённых",
)
async def clean_news_channel_cmd(interaction: discord.Interaction):
    if interaction.user.id not in ADMIN_USER_IDS:
        await interaction.response.send_message(
            "Эта команда доступна только администраторам бота.", ephemeral=True
        )
        return

    global _news_channel
    if _news_channel is None:
        _news_channel = await _resolve_news_channel()
    if _news_channel is None:
        await interaction.response.send_message("Канал news не найден.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    pinned_ids = {m.id for m in await _news_channel.pins()}
    deleted = await _news_channel.purge(
        limit=None,
        check=lambda m: m.author == bot.user and m.id not in pinned_ids,
    )

    await interaction.followup.send(
        f"Удалено старых сообщений: {len(deleted)}. Панель настроек сохранена.",
        ephemeral=True,
    )


@bot.tree.command(name="mysubscriptions", description="Показать текущие подписки на валюты")
async def my_subscriptions_cmd(interaction: discord.Interaction):
    record = await _get_subscription(interaction.user.id)
    currencies_text = ", ".join(sorted(record["currencies"])) if record["currencies"] else "нет подписок"
    lead_text = ", ".join(f"{m} мин" for m in sorted(record["lead_minutes"])) if record["lead_minutes"] else "не выбрано"
    await interaction.response.send_message(
        f"Валюты: **{currencies_text}**\nТайминг уведомлений: **{lead_text}**",
        ephemeral=True,
    )


@bot.tree.command(name="unsubscribe_all", description="Отключить все уведомления")
async def unsubscribe_all_cmd(interaction: discord.Interaction):
    await _delete_subscription(interaction.user.id)
    await interaction.response.send_message("Все подписки отключены.", ephemeral=True)


# ── Фоновые задачи ─────────────────────────────────────────────────

@tasks.loop(minutes=REFRESH_CALENDAR_MINUTES)
async def refresh_calendar_task():
    global _events_cache
    try:
        loop = asyncio.get_running_loop()
        _events_cache = await loop.run_in_executor(None, fetch_calendar, TARGET_TZ_NAME)
        log.info("Календарь обновлён: %d событий", len(_events_cache))
    except Exception:
        log.exception("Не удалось обновить календарь")


@tasks.loop(minutes=10)
async def keepalive_ping_task():
    if not SELF_PING_URL:
        return
    try:
        resp = requests.get(SELF_PING_URL, timeout=30)
        log.debug("Keep-alive пинг: HTTP %d", resp.status_code)
    except Exception:
        log.warning("Keep-alive пинг не удался для %s", SELF_PING_URL)


@tasks.loop(seconds=CHECK_INTERVAL_SECONDS)
async def check_and_post_task():
    now = datetime.now(TARGET_TZ)

    impact_order = {"Low": 0, "Medium": 1, "High": 2, "Holiday": -1}
    threshold = impact_order.get(MIN_IMPACT, 2)

    for e in _events_cache:
        if impact_order.get(e.get("impact"), -1) < threshold:
            continue

        # персональные уведомления — по индивидуальным таймингам пользователей.
        # В общий канал news ничего не постится: там только панель настроек.
        await _notify_subscribers(e, now)


@refresh_calendar_task.before_loop
@check_and_post_task.before_loop
async def _before_loops():
    await bot.wait_until_ready()


@bot.event
async def on_ready():
    log.info("Бот запущен как %s", bot.user)

    try:
        if GUILD_ID:
            guild_obj = discord.Object(id=GUILD_ID)
            bot.tree.copy_global_to(guild=guild_obj)
            synced = await bot.tree.sync(guild=guild_obj)
        else:
            synced = await bot.tree.sync()
        log.info("Слэш-команды синхронизированы: %d", len(synced))
    except Exception:
        log.exception("Не удалось синхронизировать слэш-команды")

    if not refresh_calendar_task.is_running():
        refresh_calendar_task.start()
    if not check_and_post_task.is_running():
        check_and_post_task.start()
    if not keepalive_ping_task.is_running():
        keepalive_ping_task.start()

    await _refresh_panel_on_startup()


if __name__ == "__main__":
    # Мини веб-сервер для health check (нужен Render free: без него
    # сервис засыпает и рвёт WebSocket-соединение с Discord).
    class _HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *args):
            pass

    port = int(os.environ.get("PORT", "8000"))
    _health_server = ThreadingHTTPServer(("0.0.0.0", port), _HealthHandler)
    threading.Thread(target=_health_server.serve_forever, daemon=True).start()
    log.info("Health check сервер запущен на порту %d", port)

    bot.run(DISCORD_TOKEN)
