from __future__ import annotations

import asyncio
import html
import time
import uuid
from dataclasses import dataclass

import httpx
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandObject
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from .settings import Settings


@dataclass
class PendingAction:
    id: str
    user_id: int
    target_id: str
    action: str
    expires_at: float
    confirmations: int = 0


class ControllerApi:
    def __init__(self, base_url: str, token: str):
        self.http = httpx.AsyncClient(
            base_url=base_url, timeout=120,
            headers={"Authorization": f"Bearer {token}"},
        )

    async def get(self, path: str, **params):
        response = await self.http.get(path, params=params)
        response.raise_for_status()
        return response.json()

    async def action(self, target: str, action: str, actor: str):
        response = await self.http.post(
            f"/api/v1/targets/{target}/{action}", json={"actor": actor, "reason": "telegram"}
        )
        response.raise_for_status()
        return response.json()


def format_status(payload: dict, target_filter: str | None = None) -> str:
    states = {row["target_id"]: row for row in payload.get("targets", [])}
    generations = payload.get("generations", [])
    targets = [target_filter] if target_filter else sorted(states)
    lines = [f"Mode: {'DRY-RUN' if payload.get('dry_run') else 'LIVE'}"]
    for target in targets:
        state = states.get(target, {})
        active_id = state.get("active_generation_id")
        active = next((item for item in generations if item["id"] == active_id), None)
        if active:
            gib = active["bytes_sent"] / 1024 ** 3
            lines.append(f"{target}: {active['state']} | {active['fqdn']} | {gib:.2f} GiB")
        else:
            lines.append(f"{target}: not imported")
    return "\n".join(lines)


def format_location(chat_id: int, thread_id: int | None) -> str:
    return f"chat_id: {chat_id}\nmessage_thread_id: {thread_id if thread_id is not None else 'none'}"


async def run_bot(settings: Settings) -> None:
    if not settings.telegram_bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is empty")
    if not settings.controller_token:
        raise RuntimeError("CONTROLLER_TOKEN is empty")

    bot = Bot(settings.telegram_bot_token)
    dispatcher = Dispatcher()
    router = Router()
    api = ControllerApi(settings.controller_url, settings.controller_token)
    pending: dict[str, PendingAction] = {}

    def allowed(user_id: int | None) -> bool:
        return user_id is not None and user_id in settings.allowed_user_ids

    async def deny(message_or_query):
        if isinstance(message_or_query, Message):
            await message_or_query.answer("Access denied")
        else:
            await message_or_query.answer("Access denied", show_alert=True)

    @router.message(Command("start", "help"))
    async def help_command(message: Message):
        if not allowed(message.from_user.id if message.from_user else None):
            return await deny(message)
        await message.answer(
            "/status /targets /target ID /traffic ID /history ID /alerts /whereami\n"
            "/prepare ID /rotate ID /recreate ID /pause ID /resume ID /recheck ID /rollback ID /cleanup ID"
        )

    @router.message(Command("whereami"))
    async def whereami_command(message: Message):
        if not allowed(message.from_user.id if message.from_user else None):
            return await deny(message)
        await message.answer(format_location(message.chat.id, message.message_thread_id))

    @router.message(Command("status", "targets"))
    async def status_command(message: Message):
        if not allowed(message.from_user.id if message.from_user else None):
            return await deny(message)
        await message.answer(format_status(await api.get("/api/v1/status")))

    @router.message(Command("target", "traffic"))
    async def target_command(message: Message, command: CommandObject):
        if not allowed(message.from_user.id if message.from_user else None):
            return await deny(message)
        if not command.args:
            return await message.answer("Specify target ID")
        await message.answer(format_status(await api.get("/api/v1/status"), command.args.strip()))

    @router.message(Command("history", "alerts"))
    async def history_command(message: Message, command: CommandObject):
        if not allowed(message.from_user.id if message.from_user else None):
            return await deny(message)
        target = command.args.strip() if command.args else None
        events = await api.get("/api/v1/events", target=target, limit=20)
        text = "\n".join(f"[{e['severity']}] {e['kind']}: {e['message']}" for e in events) or "No events"
        await message.answer(text[:4000])

    @router.message(Command("pause", "resume", "recheck"))
    async def simple_action(message: Message, command: CommandObject):
        if not allowed(message.from_user.id if message.from_user else None):
            return await deny(message)
        if not command.args:
            return await message.answer("Specify target ID")
        action = {"recheck": "reconcile"}.get(command.command, command.command)
        result = await api.action(command.args.strip(), action, f"telegram:{message.from_user.id}")
        await message.answer(f"Done: {html.escape(str(result))}"[:4000])

    @router.message(Command("prepare", "rotate", "recreate", "rollback", "cleanup"))
    async def dangerous_action(message: Message, command: CommandObject):
        user_id = message.from_user.id if message.from_user else None
        if not allowed(user_id):
            return await deny(message)
        if not command.args:
            return await message.answer("Specify target ID")
        target = command.args.strip()
        if any(item.target_id == target and item.expires_at >= time.time() for item in pending.values()):
            return await message.answer(f"Another operation for {target} is awaiting confirmation")
        action_id = uuid.uuid4().hex[:12]
        pending[action_id] = PendingAction(action_id, user_id, target, command.command, time.time() + 60)
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Confirm", callback_data=f"confirm:{action_id}"),
            InlineKeyboardButton(text="Cancel", callback_data=f"cancel:{action_id}"),
        ]])
        extra = "\nCleanup requires a second confirmation." if command.command == "cleanup" else ""
        await message.answer(f"Confirm /{command.command} for {target}?{extra}", reply_markup=keyboard)

    @router.callback_query(F.data.startswith("confirm:") | F.data.startswith("cancel:"))
    async def callback(query: CallbackQuery):
        user_id = query.from_user.id
        verb, action_id = query.data.split(":", 1)
        action = pending.get(action_id)
        if not action or action.expires_at < time.time() or action.user_id != user_id:
            pending.pop(action_id, None)
            return await query.answer("Confirmation expired or belongs to another user", show_alert=True)
        if verb == "cancel":
            pending.pop(action_id, None)
            await query.message.edit_text("Cancelled")
            return await query.answer()
        if action.action == "cleanup" and action.confirmations == 0:
            action.confirmations = 1
            action.expires_at = time.time() + 60
            await query.message.edit_text(
                f"Second confirmation: preview retired resources for {action.target_id}?",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="Confirm again", callback_data=f"confirm:{action_id}"),
                    InlineKeyboardButton(text="Cancel", callback_data=f"cancel:{action_id}"),
                ]]),
            )
            return await query.answer()
        try:
            if action.action == "cleanup":
                result = await api.get(f"/api/v1/targets/{action.target_id}/cleanup-preview")
            else:
                result = await api.action(action.target_id, action.action, f"telegram:{user_id}")
            await query.message.edit_text(f"Done: {html.escape(str(result))}"[:4000])
        except httpx.HTTPError as exc:
            await query.message.edit_text(f"Failed: {html.escape(str(exc))}"[:4000])
        finally:
            pending.pop(action_id, None)
        await query.answer()

    dispatcher.include_router(router)
    await dispatcher.start_polling(bot)
