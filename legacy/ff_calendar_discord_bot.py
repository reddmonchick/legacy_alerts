"""
Парсер экономического календаря Forex Factory + публикация в Discord.

Источник: официальный (хоть и "неофициальный") JSON-фид,
который сама Forex Factory отдаёт для EA/индикаторов MT4/MT5.
Время в фиде — America/New_York (Eastern Time).

- В канал news постятся ВСЕ high-impact новости за час до выхода.
- Через панель с выпадающими меню в канале news пользователь выбирает
  валюты (включая "Все валюты") и тайминг(и) уведомлений (5/15/30/60 мин).
  Бот создаёт ему персональный тред внутри канала news и пишет туда
  только события по выбранным валютам, в выбранное время. Подписки
  хранятся в PostgreSQL.

pip install requests python-dateutil pytz discord.py python-decouple asyncpg
"""

import asyncio
import logging
from datetime import datetime, timedelta

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

MIN_IMPACT = "High"
LEAD_TIME = timedelta(hours=1)              # фиксированный тайминг для общего канала news
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
ALL_CURRENCIES_VALUE = "ALL"

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
        self.add_view(CurrencyFilterView())


bot = CalendarBot(command_prefix="!", intents=intents)

_events_cache: list[dict] = []
_posted_keys: set[tuple] = set()
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
            await thread.add_user(user)
        except discord.HTTPException:
            log.exception("Не удалось создать тред для %s", user)
            return None

        await _delete_thread_created_notice()
        await _set_thread_id(user.id, thread.id)
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
            window_start = lead_td - timedelta(seconds=CHECK_INTERVAL_SECONDS)
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


# ── Панель выбора валют (постоянное меню, без слэш-команд для юзера) ──

class CurrencyFilterSelect(discord.ui.Select):
    """
    custom_id фиксированный (не привязан к пользователю) — это то,
    что делает view persistent: после рестарта бот регистрирует
    её заново в setup_hook, и старое сообщение продолжает работать.
    """
    def __init__(self):
        options = [
            discord.SelectOption(label="Выбрать все", value=ALL_CURRENCIES_VALUE),
        ] + [
            discord.SelectOption(label=cur, value=cur, emoji=CURRENCY_FLAG[cur])
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
            # если выбрано "Выбрать все" — разворачиваем в полный список кодов,
            # сам маркер ALL в базу не пишем (иначе сравнение e['country'] сломается)
            if ALL_CURRENCIES_VALUE in self.values:
                currencies = list(CURRENCY_OPTIONS)
            else:
                currencies = list(self.values)

            await _set_currencies(user.id, currencies)
            await _get_or_create_user_thread(user)
        else:
            await _delete_subscription(user.id)

        # тихое подтверждение: просто перерисовываем ту же панель без изменений,
        # без всплывающих сообщений и текста — чтобы не флудить
        await interaction.response.edit_message()


class LeadTimeSelect(discord.ui.Select):
    """Выбор одного или нескольких таймингов для персональных уведомлений."""
    def __init__(self):
        options = [
            discord.SelectOption(label=f"За {m} мин.", value=str(m))
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
        await interaction.response.edit_message()


class CurrencyFilterView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)  # timeout=None обязателен для persistent view
        self.add_item(CurrencyFilterSelect())
        self.add_item(LeadTimeSelect())


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

    embed = discord.Embed(
        title="📊 Фильтр по валютам",
        description=(
            "Выберите ниже валюты, за которыми хотите следить, и тайминг уведомлений — "
            "и я буду присылать вам уведомления в личную ветку "
            "перед важными (High Impact) новостями по ним."
        ),
        color=discord.Color.blurple(),
    )
    msg = await _news_channel.send(embed=embed, view=CurrencyFilterView())
    try:
        await msg.pin()
    except discord.HTTPException:
        log.warning("Не удалось закрепить панель — сделайте это вручную")

    await interaction.response.send_message("Панель опубликована и закреплена.", ephemeral=True)


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


@tasks.loop(seconds=CHECK_INTERVAL_SECONDS)
async def check_and_post_task():
    global _news_channel
    if _news_channel is None:
        _news_channel = await _resolve_news_channel()

    now = datetime.now(TARGET_TZ)
    window_start = LEAD_TIME - timedelta(seconds=CHECK_INTERVAL_SECONDS)

    impact_order = {"Low": 0, "Medium": 1, "High": 2, "Holiday": -1}
    threshold = impact_order.get(MIN_IMPACT, 2)

    for e in _events_cache:
        if impact_order.get(e.get("impact"), -1) < threshold:
            continue

        # публикация в общий канал news — всегда за фиксированный LEAD_TIME (1 час)
        key = _event_key(e)
        if key not in _posted_keys:
            delta = e["datetime"] - now
            if window_start <= delta <= LEAD_TIME:
                if _news_channel is not None:
                    try:
                        await _news_channel.send(embed=_make_embed(e))
                    except discord.HTTPException:
                        log.exception("Ошибка отправки сообщения в канал")
                else:
                    log.warning("Канал news не найден — пропускаю публикацию в канал")
                _posted_keys.add(key)
                log.info("Опубликовано в канал: %s (%s)", e["title"], e["country"])

        # персональные уведомления — независимо, по индивидуальным таймингам пользователей
        await _notify_subscribers(e, now)

    if len(_posted_keys) > 500:
        _posted_keys.clear()


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


if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)