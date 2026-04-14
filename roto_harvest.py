import asyncio
import discord
import os
import aiohttp
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pytz

# ─── Config ────────────────────────────────────────────────────────────────────

TOKEN       = os.environ['DISCORD_TOKEN']
CHANNEL_ID  = 1381126985014710282
SAVE_PATH   = Path('/Users/jarvis/.openclaw/workspace/media/ml_mafia/winners/')
LOG_PATH    = Path('/Users/jarvis/.openclaw/workspace/logs/roto-harvest.log')
EST         = pytz.timezone('US/Eastern')
HISTORY_LIMIT   = 500
LOOKBACK_HOURS  = 24
MAX_RETRIES     = 3
RETRY_DELAY_S   = 5   # base seconds between download retries

# No watchlist filter — #made-men-winners is a winners-only channel;
# all image attachments from any poster are harvested.

# ─── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    filename=str(LOG_PATH),
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger('roto')

# ─── Helpers ───────────────────────────────────────────────────────────────────

async def download_with_retry(session: aiohttp.ClientSession, url: str, dest: Path) -> bool:
    """Download url → dest with up to MAX_RETRIES attempts. Returns True on success."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    dest.write_bytes(await resp.read())
                    return True
                elif resp.status == 403:
                    # CDN URL expired — not retryable
                    log.warning('CDN URL expired (403) for %s — skipping', dest.name)
                    return False
                else:
                    log.warning('Attempt %d/%d — HTTP %d for %s', attempt, MAX_RETRIES, resp.status, dest.name)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            log.warning('Attempt %d/%d — network error downloading %s: %s', attempt, MAX_RETRIES, dest.name, exc)

        if attempt < MAX_RETRIES:
            await asyncio.sleep(RETRY_DELAY_S * attempt)

    log.error('All %d download attempts failed for %s', MAX_RETRIES, dest.name)
    return False


# ─── Main ──────────────────────────────────────────────────────────────────────

async def main():
    log.info('=== Roto harvest started ===')

    intents = discord.Intents.default()
    intents.message_content = True

    client = discord.Client(intents=intents)
    saved  = 0
    errors = 0

    try:
        await client.login(TOKEN)
    except discord.LoginFailure as exc:
        log.error('Discord login failed: %s', exc)
        return
    except Exception as exc:
        log.error('Unexpected error during login: %s', exc)
        return

    try:
        channel = await client.fetch_channel(CHANNEL_ID)
    except discord.Forbidden:
        log.error('Bot lacks permission to access channel %d', CHANNEL_ID)
        await client.close()
        return
    except discord.NotFound:
        log.error('Channel %d not found', CHANNEL_ID)
        await client.close()
        return
    except Exception as exc:
        log.error('Failed to fetch channel %d: %s', CHANNEL_ID, exc)
        await client.close()
        return

    today       = datetime.now(EST).strftime('%Y-%m-%d')
    save_dir    = SAVE_PATH / today
    cutoff_utc  = datetime.now(pytz.utc) - timedelta(hours=LOOKBACK_HOURS)

    try:
        save_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        log.error('Cannot create save directory %s: %s', save_dir, exc)
        await client.close()
        return

    log.info('Saving to: %s', save_dir)

    try:
        async with aiohttp.ClientSession() as session:
            async for message in channel.history(limit=HISTORY_LIMIT, oldest_first=False):
                # Skip messages outside the lookback window
                if message.created_at < cutoff_utc:
                    break

                for attachment in message.attachments:
                    if not any(attachment.filename.lower().endswith(ext)
                               for ext in ('.jpg', '.jpeg', '.png', '.gif')):
                        continue

                    est_time  = message.created_at.astimezone(EST)
                    time_str  = est_time.strftime('%I.%M%p')
                    dest_name = f'{message.author.name}_{time_str}.png'
                    dest_path = save_dir / dest_name

                    if dest_path.exists():
                        # Deduplicate same-minute posts
                        idx = 2
                        while (save_dir / f'{message.author.name}_{time_str}_{idx}.png').exists():
                            idx += 1
                        dest_path = save_dir / f'{message.author.name}_{time_str}_{idx}.png'
                        dest_name = dest_path.name

                    ok = await download_with_retry(session, attachment.url, dest_path)
                    if ok:
                        log.info('Saved: %s', dest_name)
                        saved += 1
                    else:
                        errors += 1

    except discord.HTTPException as exc:
        log.error('Discord API error while reading history: %s (status=%s)', exc.text, exc.status)
    except Exception as exc:
        log.error('Unexpected error during harvest: %s', exc, exc_info=True)
    finally:
        await client.close()

    log.info('=== Roto harvest complete — saved: %d, errors: %d ===', saved, errors)


if __name__ == '__main__':
    asyncio.run(main())
