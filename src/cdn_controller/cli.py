from __future__ import annotations

import argparse
import asyncio
import json

from .clients import remnawave_client
from .controller import Controller
from .db import Database
from .models import load_config
from .settings import Settings


async def execute(args) -> None:
    settings = Settings()
    config = load_config(settings.config_path)
    db = Database(settings.database_path)
    controller = Controller(config, settings, db)
    await controller.initialize()
    try:
        if args.command == "validate":
            target = config.target(args.target) if args.target else None
            provider_checks = {"yandex_iam": "not-checked-in-dry-run"}
            if not settings.dry_run:
                await controller.yandex.auth.validate(exchange_token=True)
                provider_checks["yandex_iam"] = "ok"
                targets = [target] if target else [item for item in config.targets if item.enabled]
                for item in targets:
                    await controller.cloudflare.validate_dns_access(item.domain.cloudflare_zone_id)
                    remna_provider = config.providers.remnawave[item.remnawave.panel]
                    remna = remnawave_client(remna_provider, remna_provider.token_env)
                    try:
                        host = await remna.get_host(item.remnawave.host_id)
                    finally:
                        await remna.close()
                    if str(host.get("uuid")) != item.remnawave.host_id:
                        raise RuntimeError(
                            f"Remnawave returned unexpected Host UUID for target {item.id}"
                        )
                    provider_checks[f"cloudflare:{item.id}"] = "dns-access-ok"
                    provider_checks[f"remnawave:{item.id}"] = "ok"
            print(json.dumps({
                "valid": True,
                "dry_run": settings.dry_run,
                "providers": provider_checks,
                "target": target.model_dump() if target else None,
            }, indent=2))
        elif args.command == "status":
            print(json.dumps(await controller.status(), indent=2, default=str))
        elif args.command == "reconcile":
            targets = [args.target] if args.target else [target.id for target in config.targets if target.enabled]
            print(json.dumps([await controller.reconcile(target, "cli") for target in targets], indent=2, default=str))
        elif args.command == "prepare":
            print((await controller.prepare(args.target, "cli")).model_dump_json(indent=2))
        elif args.command == "rotate":
            print(json.dumps(await controller.rotate(args.target, "cli"), indent=2, default=str))
        elif args.command == "rollback":
            print(json.dumps(await controller.rollback(args.target, "cli"), indent=2))
        elif args.command == "pause":
            await db.set_paused(args.target, True)
        elif args.command == "resume":
            await db.set_paused(args.target, False)
        elif args.command == "import-existing":
            print((await db.import_active(args.target, args.resource_id, args.fqdn, args.bytes_sent)).model_dump_json(indent=2))
        elif args.command == "cleanup":
            print(json.dumps({"candidates": [g.model_dump(mode="json") for g in await db.generations(args.target)
                                             if g.state.value == "RETIRED"], "deleted": False}, indent=2))
        elif args.command == "monitoring-test":
            await controller.notifier.kuma(settings.uptime_kuma_push_url, True, "cdn-controller test")
            print("monitoring test sent")
        elif args.command == "telegram-test":
            from .notifications import Event
            await controller.notifier.emit(Event(None, "INFO", "test", "cdn-controller Telegram test"))
            print("telegram test sent")
    finally:
        await controller.close()


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="cdnctl")
    commands = root.add_subparsers(dest="command", required=True)
    for name in ("status", "reconcile"):
        item = commands.add_parser(name)
        item.add_argument("target", nargs="?")
    validate = commands.add_parser("validate")
    validate.add_argument("target", nargs="?")
    for name in ("prepare", "rotate", "rollback", "pause", "resume", "cleanup"):
        item = commands.add_parser(name)
        item.add_argument("target")
    imported = commands.add_parser("import-existing")
    imported.add_argument("target")
    imported.add_argument("resource_id")
    imported.add_argument("fqdn")
    imported.add_argument("--bytes-sent", type=float, default=0)
    commands.add_parser("monitoring-test")
    commands.add_parser("telegram-test")
    return root


def main() -> None:
    asyncio.run(execute(parser().parse_args()))
