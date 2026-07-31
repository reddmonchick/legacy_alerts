FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ff_calendar_discord_bot.py .

CMD ["python", "ff_calendar_discord_bot.py"]