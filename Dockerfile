FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY roto_harvest.py .

# Set via runtime: -e DISCORD_TOKEN=<token>
# Production schedule: 0 7 * * * (7:00 AM EST daily)
CMD ["python3", "roto_harvest.py"]
