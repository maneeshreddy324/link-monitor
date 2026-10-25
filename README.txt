SETUP (about 10 minutes, free)
1. Create a free GitHub account, then a NEW PRIVATE repository.
2. Upload all files from this folder (keep the .github/workflows folder).
3. Edit links.txt: paste your 100+ links, one per line.
4. Repo > Settings > Secrets and variables > Actions > New repository secret. Add:
   Telegram (instant phone alert):
     TELEGRAM_BOT_TOKEN  (create a bot with @BotFather)
     TELEGRAM_CHAT_ID    (message your bot, then open
                          https://api.telegram.org/bot<TOKEN>/getUpdates)
   Email (optional):
     SMTP_USER, SMTP_PASSWORD (Gmail app password), MAIL_TO (comma separated)
5. Repo > Actions tab > "Monitor links" > Run workflow once.
   The first run only saves a baseline. Alerts start from the next run.
It then runs automatically every 5 minutes, 24/7.
To add a link later: edit links.txt on GitHub and save.
