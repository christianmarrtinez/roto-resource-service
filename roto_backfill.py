"""
roto_backfill.py — One-shot historical harvest for a date range.

Usage:
    DISCORD_TOKEN=<token> python3 roto_backfill.py [FROM_DATE] [TO_DATE]
    Dates in YYYY-MM-DD format (default: 2026-04-01 to today).
"""

import asyncio
import discord
import os
import aiohttp
import csv
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytz

# ─── Config ────────────────────────────────────────────────────────────────────

TOKEN          = os.environ['DISCORD_TOKEN']
CHANNEL_ID     = 1381126985014710282
SAVE_PATH      = Path('/Users/jarvis/.openclaw/workspace/media/ml_mafia/winners/')
LOG_PATH       = Path('/Users/jarvis/.openclaw/workspace/logs/roto-harvest.log')
WATCHLIST_PATH = Path('/Users/jarvis/.openclaw/workspace/tasks/repositories/tracked-twitter.md')
EST            = pytz.timezone('US/Eastern')
UTC            = pytz.utc
MAX_RETRIES    = 3
RETRY_DELAY_S  = 5

# ─── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    filename=str(LOG_PATH),
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
# Also mirror to stdout so we can watch live
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%H:%M:%S'))
log = logging.getLogger('roto.backfill')
log.addHandler(console)

# ─── Helpers ───────────────────────────────────────────────────────────────────

def load_watchlist() -> set:
    if not WATCHLIST_PATH.exists():
        log.error('Watchlist not found: %s', WATCHLIST_PATH)
        return set()
    try:
        with open(WATCHLIST_PATH, newline='') as f:
            names = {row['Username'].strip() for row in csv.DictReader(f)
                     if row.get('Username', '').strip()}
        log.info('Watchlist loaded — %d usernames', len(names))
        return names
    except Exception as exc:
        log.error('Failed to load watchlist: %s', exc)
        return set()


async def download_with_retry(session: aiohttp.ClientSession, url: str, dest: Path) -> bool:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    dest.write_bytes(await resp.read())
                    return True
                elif resp.status == 403:
                    log.warning('CDN URL expired (403) for %s — skipping', dest.name)
                    return False
                else:
                    log.warning('Attempt %d/%d — HTTP %d for %s', attempt, MAX_RETRIES, resp.status, dest.name)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            log.warning('Attempt %d/%d — network error for %s: %s', attempt, MAX_RETRIES, dest.name, exc)
        if attempt < MAX_RETRIES:
            await asyncio.sleep(RETRY_DELAY_S * attempt)
    log.error('All %d attempts failed for %s', MAX_RETRIES, dest.name)
    return False


# ─── Main ──────────────────────────────────────────────────────────────────────

async def main(from_date: datetime, to_date: datetime):
    log.info('=== Roto BACKFILL started: %s → %s ===',
             from_date.strftime('%Y-%m-%d'), to_date.strftime('%Y-%m-%d'))

    watchlist = load_watchlist()
    if not watchlist:
        log.warning('Empty watchlist — will collect nothing.')

    intents = discord.Intents.default()
    intents.message_content = True
    client = discord.Client(intents=intents)

    try:
        await client.login(TOKEN)
    except discord.LoginFailure as exc:
        log.error('Login failed: %s', exc)
        return
    except Exception as exc:
        log.error('Unexpected login error: %s', exc)
        return

    try:
        channel = await client.fetch_channel(CHANNEL_ID)
    except discord.Forbidden:
        log.error('No permission for channel %d', CHANNEL_ID)
        await client.close()
        return
    except discord.NotFound:
        log.error('Channel %d not found', CHANNEL_ID)
        await client.close()
        return
    except Exception as exc:
        log.error('Failed to fetch channel: %s', exc)
        await client.close()
        return

    saved  = 0
    errors = 0
    skipped_existing = 0
    daily_counts: dict[str, int] = {}

    try:
        async with aiohttp.ClientSession() as session:
            # Paginate the full range — discord.py handles chunking automatically
            async for message in channel.history(
                limit=None,
                after=from_date,
                before=to_date,
                oldest_first=True,
            ):
                if message.author.name not in watchlist:
                    continue

                # Bucket by EST date of the message
                msg_est  = message.created_at.astimezone(EST)
                day_str  = msg_est.strftime('%Y-%m-%d')
                save_dir = SAVE_PATH / day_str
                save_dir.mkdir(parents=True, exist_ok=True)

                for attachment in message.attachments:
                    if not any(attachment.filename.lower().endswith(ext)
                               for ext in ('.jpg', '.jpeg', '.png', '.gif')):
                        continue

                    time_str  = msg_est.strftime('%I.%M%p')
                    dest_name = f'{message.author.name}_{time_str}.png'
                    dest_path = save_dir / dest_name

                    if dest_path.exists():
                        log.info('Already exists, skipping: %s/%s', day_str, dest_name)
                        skipped_existing += 1
                        continue

                    ok = await download_with_retry(session, attachment.url, dest_path)
                    if ok:
                        log.info('Saved: %s/%s', day_str, dest_name)
                        saved += 1
                        daily_counts[day_str] = daily_counts.get(day_str, 0) + 1
                    else:
                        errors += 1

    except discord.HTTPException as exc:
        log.error('Discord API error: %s (status=%s)', exc.text, exc.status)
    except Exception as exc:
        log.error('Unexpected harvest error: %s', exc, exc_info=True)
    finally:
        await client.close()

    log.info('--- Per-day breakdown ---')
    for day in sorted(daily_counts):
        log.info('  %s: %d image(s)', day, daily_counts[day])

    log.info('=== Backfill complete — saved: %d, errors: %d, already existed: %d ===',
             saved, errors, skipped_existing)
    return saved, errors, daily_counts


if __name__ == '__main__':
    # Parse optional CLI date args
    try:
        from_arg = sys.argv[1] if len(sys.argv) > 1 else '2026-04-01'
        to_arg   = sys.argv[2] if len(sys.argv) > 2 else datetime.now(UTC).strftime('%Y-%m-%d')

        from_dt = UTC.localize(datetime.strptime(from_arg, '%Y-%m-%d'))
        # to_date is exclusive end-of-day
        to_dt   = UTC.localize(datetime.strptime(to_arg, '%Y-%m-%d') + timedelta(hours=23, minutes=59, seconds=59))
    except ValueError as e:
        print(f'Invalid date format: {e}. Use YYYY-MM-DD.')
        sys.exit(1)

    asyncio.run(main(from_dt, to_dt))
