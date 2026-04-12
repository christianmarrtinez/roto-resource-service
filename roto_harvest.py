import asyncio
import discord
import os
import aiohttp
import csv
from datetime import datetime
import pytz

TOKEN = '${DISCORD_TOKEN}'
CHANNEL_ID = 1381126985014710282
SAVE_PATH = '/Users/jarvis/.openclaw/workspace/media/ml_mafia/winners/'
EST = pytz.timezone('US/Eastern')

async def main():
    # Load watchlist from tracked-twitter.md
    watchlist = set()
    with open('/Users/jarvis/.openclaw/workspace/tasks/repositories/tracked-twitter.md', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['Username']:
                watchlist.add(row['Username'])

    intents = discord.Intents.all()
    async with discord.Client(intents=intents) as client:
        await client.login(TOKEN)
        channel = await client.fetch_channel(CHANNEL_ID)

        today = datetime.now(EST).strftime('%Y-%m-%d')
        full_path = os.path.join(SAVE_PATH, today)
        if not os.path.exists(full_path):
            os.makedirs(full_path)

        async for message in channel.history(limit=500):
            # AND LOGIC: Must be in watchlist AND posted in last 24h
            if message.author.name in watchlist and (datetime.now(pytz.utc) - message.created_at).total_seconds() < 86400:
                for attachment in message.attachments:
                    if any(attachment.filename.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif']):
                        est_time = message.created_at.astimezone(EST)
                        time_str = est_time.strftime('%I.%M%p')
                        new_filename = f'{message.author.name}_{time_str}.png'
                        file_path = os.path.join(full_path, new_filename)

                        async with aiohttp.ClientSession() as session:
                            async with session.get(attachment.url) as resp:
                                if resp.status == 200:
                                    with open(file_path, 'wb') as f:
                                        f.write(await resp.read())
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
