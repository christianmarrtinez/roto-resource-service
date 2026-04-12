# roto-resource-service

Harvests daily winner screenshots from the ML Mafia Discord channel and saves them locally using a `{Username}_{Time}.png` (EST) naming convention.

## What It Does

- Connects to Discord channel `1381126985014710282`
- Reads the watchlist from `tracked-twitter.md`
- Pulls image attachments from users on the watchlist posted within the last 24 hours
- Saves files to `/Users/jarvis/.openclaw/workspace/media/ml_mafia/winners/YYYY-MM-DD/`
- Filenames follow: `{Username}_{HH.MMam/pm}.png` (time in EST)

## Production Schedule

```
0 7 * * * DISCORD_TOKEN=<token> /usr/bin/python3 /Users/jarvis/.openclaw/workspace/roto-resource-service/roto_harvest.py >> /Users/jarvis/.openclaw/workspace/logs/roto-harvest.log 2>&1
```

Runs at **7:00 AM EST daily**.

## Setup

```bash
pip install -r requirements.txt
python3 roto_harvest.py
```

## Docker

```bash
docker build -t roto-resource-service .
docker run --rm \
  -v /Users/jarvis/.openclaw/workspace/media:/Users/jarvis/.openclaw/workspace/media \
  -v /Users/jarvis/.openclaw/workspace/tasks:/Users/jarvis/.openclaw/workspace/tasks \
  roto-resource-service
```

## Dependencies

- `discord.py` — Discord API client with `Intents.all()` for message content access
- `aiohttp` — async HTTP for downloading attachments
- `pytz` — EST timezone handling
