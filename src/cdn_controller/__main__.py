import asyncio
import json
import logging
import sys
from datetime import UTC, datetime

import uvicorn

from .api import create_app
from .controller import Controller
from .db import Database
from .models import load_config
from .settings import Settings
from .telegram_bot import run_bot


def configure_logging() -> None:
    class JsonFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            payload = {
                "timestamp": datetime.now(UTC).isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            }
            if record.exc_info:
                payload["exception"] = self.formatException(record.exc_info)
            return json.dumps(payload, ensure_ascii=False)

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
    # httpx logs full request URLs. Telegram embeds the bot token in its URL,
    # so INFO request logging would disclose the credential to stdout/log collectors.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def main() -> None:
    configure_logging()
    mode = sys.argv[1] if len(sys.argv) > 1 else "controller"
    settings = Settings()
    if mode == "telegram":
        asyncio.run(run_bot(settings))
        return
    if mode == "cli":
        from .cli import main as cli_main
        sys.argv = [sys.argv[0], *sys.argv[2:]]
        cli_main()
        return
    config = load_config(settings.config_path)
    db = Database(settings.database_path)
    controller = Controller(config, settings, db)
    uvicorn.run(create_app(controller, config, settings), host="0.0.0.0", port=9187)


if __name__ == "__main__":
    main()
