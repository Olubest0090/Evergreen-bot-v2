"""
Evergreen Bot v2 – central database layer.

Talks to Supabase (PostgREST) over HTTPS using the service key.
All public functions either return clean data or raise DatabaseError.
HTTP errors are never treated as empty/successful results.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import aiohttp

log = logging.getLogger("evergreen.database")

_session: aiohttp.ClientSession | None = None
_base_url: str = ""


class DatabaseError(Exception):
    """Raised on any Supabase request failure."""


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

async def init(timeout: float = 15.0) -> None:
    """Create the shared aiohttp session. Call once at bot startup."""
    global _session, _base_url

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        raise DatabaseError("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")

    _base_url = url.rstrip("/")
    _session = aiohttp.ClientSession(
        headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        },
        timeout=aiohttp.ClientTimeout(total=timeout),
    )
    log.info("Supabase client initialised")


async def close() -> None:
    """Close the shared session. Call on bot shutdown."""
    global _session
    if _session and not _session.closed:
        await _session.close()
        _session = None
        log.info("Supabase client closed")


# ---------------------------------------------------------------------------
# Low-level request helper
# ---------------------------------------------------------------------------

async def _request(
    method: str,
    path: str,
    *,
    params: dict[str, str] | None = None,
    json_body: Any = None,
    prefer: str | None = None,
) -> Any:
    """
    Perform a single request against Supabase REST API.
    Raises DatabaseError on any non-2xx response or network failure.
    """
    if _session is None or _session.closed:
        raise DatabaseError("Database client is not initialised – call init() first")

    url = f"{_base_url}/rest/v1/{path.lstrip('/')}"
    headers = {}
    if prefer:
        headers["Prefer"] = prefer

    try:
        async with _session.request(
            method.upper(),
            url,
            params=params,
            json=json_body,
            headers=headers or None,
        ) as resp:
            text = await resp.text()

            if resp.status not in (200, 201, 204):
                log.error(
                    "Supabase %s %s → %s: %s",
                    method.upper(),
                    path,
                    resp.status,
                    text[:500],
                )
                raise DatabaseError(f"Supabase {resp.status}: {text[:300]}")

            if resp.status == 204 or not text:
                return None

            try:
                return json.loads(text)
            except json.JSONDecodeError as exc:
                raise DatabaseError(f"Invalid JSON from Supabase: {exc}") from exc

    except aiohttp.ClientError as exc:
        log.exception("Network error talking to Supabase")
        raise DatabaseError(f"Network error: {exc}") from exc


# ---------------------------------------------------------------------------
# Government roles
# ---------------------------------------------------------------------------

async def set_guild_role(guild_id: int, role_type: str, role_id: int) -> None:
    await _request(
        "POST",
        "guild_roles",
        json_body={"guild_id": guild_id, "role_type": role_type, "role_id": role_id},
        prefer="resolution=merge-duplicates,return=minimal",
    )


async def get_guild_role(guild_id: int, role_type: str) -> int | None:
    data = await _request(
        "GET",
        "guild_roles",
        params={
            "guild_id": f"eq.{guild_id}",
            "role_type": f"eq.{role_type}",
            "select": "role_id",
        },
    )
    return data[0]["role_id"] if data else None


async def get_all_guild_roles(guild_id: int) -> dict[str, int]:
    data = await _request(
        "GET",
        "guild_roles",
        params={"guild_id": f"eq.{guild_id}", "select": "role_type,role_id"},
    )
    return {row["role_type"]: row["role_id"] for row in (data or [])}


# ---------------------------------------------------------------------------
# Alerts config
# ---------------------------------------------------------------------------

async def _upsert_alerts_config(guild_id: int, **fields: Any) -> None:
    await _request(
        "POST",
        "alerts_config",
        params={"on_conflict": "guild_id"},
        json_body={"guild_id": guild_id, **fields},
        prefer="resolution=merge-duplicates,return=minimal",
    )


async def set_alliance_id(guild_id: int, alliance_id: int) -> None:
    await _upsert_alerts_config(guild_id, alliance_id=alliance_id)


async def set_alert_channel(guild_id: int, column: str, channel_id: int) -> None:
    await _upsert_alerts_config(guild_id, **{column: channel_id})


async def get_alerts_config(guild_id: int) -> dict | None:
    data = await _request(
        "GET",
        "alerts_config",
        params={"guild_id": f"eq.{guild_id}", "select": "*"},
    )
    return data[0] if data else None


async def get_all_alerts_configs() -> list[dict]:
    data = await _request("GET", "alerts_config", params={"select": "*"})
    return data or []


# ---------------------------------------------------------------------------
# Nation links (global per Discord user)
# ---------------------------------------------------------------------------

async def link_nation(guild_id: int, discord_user_id: int, nation_id: int) -> None:
    await _request(
        "POST",
        "nation_links",
        params={"on_conflict": "discord_user_id"},
        json_body={
            "discord_user_id": discord_user_id,
            "nation_id": nation_id,
            "guild_id": guild_id,
        },
        prefer="resolution=merge-duplicates,return=minimal",
    )


async def unlink_nation(discord_user_id: int) -> None:
    await _request(
        "DELETE",
        "nation_links",
        params={"discord_user_id": f"eq.{discord_user_id}"},
        prefer="return=minimal",
    )


async def get_nation_for_user(discord_user_id: int) -> int | None:
    data = await _request(
        "GET",
        "nation_links",
        params={
            "discord_user_id": f"eq.{discord_user_id}",
            "select": "nation_id",
        },
    )
    return data[0]["nation_id"] if data else None


async def get_discord_id_for_nation(nation_id: int) -> int | None:
    data = await _request(
        "GET",
        "nation_links",
        params={"nation_id": f"eq.{nation_id}", "select": "discord_user_id"},
    )
    return data[0]["discord_user_id"] if data else None


# ---------------------------------------------------------------------------
# Seen wars (per-guild)
# ---------------------------------------------------------------------------

async def is_war_alerted(guild_id: int, war_id: int, field: str) -> bool:
    data = await _request(
        "GET",
        "seen_wars",
        params={
            "guild_id": f"eq.{guild_id}",
            "war_id": f"eq.{war_id}",
            "select": field,
        },
    )
    return bool(data and data[0].get(field))


async def mark_war_alerted(guild_id: int, war_id: int, field: str) -> None:
    await _request(
        "POST",
        "seen_wars",
        params={"on_conflict": "guild_id,war_id"},
        json_body={"guild_id": guild_id, "war_id": war_id, field: True},
        prefer="resolution=merge-duplicates,return=minimal",
    )


# ---------------------------------------------------------------------------
# Spy tracking (per-guild)
# ---------------------------------------------------------------------------

async def get_last_spies(guild_id: int, nation_id: int) -> int | None:
    data = await _request(
        "GET",
        "nation_spy_tracking",
        params={
            "guild_id": f"eq.{guild_id}",
            "nation_id": f"eq.{nation_id}",
            "select": "last_known_spies",
        },
    )
    return data[0]["last_known_spies"] if data else None


async def set_last_spies(guild_id: int, nation_id: int, spies: int) -> None:
    await _request(
        "POST",
        "nation_spy_tracking",
        params={"on_conflict": "guild_id,nation_id"},
        json_body={
            "guild_id": guild_id,
            "nation_id": nation_id,
            "last_known_spies": spies,
        },
        prefer="resolution=merge-duplicates,return=minimal",
    )


# ---------------------------------------------------------------------------
# DNR settings
# ---------------------------------------------------------------------------

async def set_dnr_top_x(guild_id: int, top_x: int) -> None:
    await _request(
        "POST",
        "dnr_settings",
        params={"on_conflict": "guild_id"},
        json_body={"guild_id": guild_id, "top_x": top_x},
        prefer="resolution=merge-duplicates,return=minimal",
    )


async def get_dnr_top_x(guild_id: int) -> int:
    data = await _request(
        "GET",
        "dnr_settings",
        params={"guild_id": f"eq.{guild_id}", "select": "top_x"},
    )
    return data[0]["top_x"] if data else 0


# ---------------------------------------------------------------------------
# Coalitions
# ---------------------------------------------------------------------------

async def add_coalition_alliance(
    guild_id: int,
    alliance_id: int,
    alliance_name: str,
    bloc_type: str,
    auto_synced: bool = False,
) -> None:
    await _request(
        "POST",
        "coalitions",
        params={"on_conflict": "guild_id,alliance_id,bloc_type"},
        json_body={
            "guild_id": guild_id,
            "alliance_id": alliance_id,
            "alliance_name": alliance_name,
            "bloc_type": bloc_type,
            "auto_synced": auto_synced,
        },
        prefer="resolution=merge-duplicates,return=minimal",
    )


async def remove_coalition_alliance(
    guild_id: int, alliance_id: int, bloc_type: str
) -> None:
    await _request(
        "DELETE",
        "coalitions",
        params={
            "guild_id": f"eq.{guild_id}",
            "alliance_id": f"eq.{alliance_id}",
            "bloc_type": f"eq.{bloc_type}",
        },
        prefer="return=minimal",
    )


async def list_coalitions(
    guild_id: int, bloc_type: str | None = None
) -> list[dict]:
    params: dict[str, str] = {"guild_id": f"eq.{guild_id}", "select": "*"}
    if bloc_type:
        params["bloc_type"] = f"eq.{bloc_type}"
    data = await _request("GET", "coalitions", params=params)
    return data or []


async def clear_auto_synced_coalitions(guild_id: int) -> int:
    data = await _request(
        "DELETE",
        "coalitions",
        params={"guild_id": f"eq.{guild_id}", "auto_synced": "eq.true"},
        prefer="return=representation",
    )
    return len(data) if isinstance(data, list) else 0


async def is_alliance_in_bloc(
    guild_id: int, alliance_id: int, bloc_type: str
) -> bool:
    data = await _request(
        "GET",
        "coalitions",
        params={
            "guild_id": f"eq.{guild_id}",
            "alliance_id": f"eq.{alliance_id}",
            "bloc_type": f"eq.{bloc_type}",
            "select": "alliance_id",
        },
    )
    return bool(data)


# ---------------------------------------------------------------------------
# Bot flags
# ---------------------------------------------------------------------------

async def mark_flag_used(guild_id: int, flag_name: str) -> None:
    await _request(
        "POST",
        "bot_flags",
        params={"on_conflict": "guild_id,flag_name"},
        json_body={"guild_id": guild_id, "flag_name": flag_name},
        prefer="resolution=merge-duplicates,return=minimal",
    )


async def is_flag_used(guild_id: int, flag_name: str) -> bool:
    data = await _request(
        "GET",
        "bot_flags",
        params={
            "guild_id": f"eq.{guild_id}",
            "flag_name": f"eq.{flag_name}",
            "select": "flag_name",
        },
    )
    return bool(data)


# ---------------------------------------------------------------------------
# War room settings
# ---------------------------------------------------------------------------

async def set_warroom_enabled(guild_id: int, enabled: bool) -> None:
    await _request(
        "POST",
        "warroom_settings",
        params={"on_conflict": "guild_id"},
        json_body={"guild_id": guild_id, "enabled": enabled},
        prefer="resolution=merge-duplicates,return=minimal",
    )


async def get_warroom_settings(guild_id: int) -> dict | None:
    data = await _request(
        "GET",
        "warroom_settings",
        params={"guild_id": f"eq.{guild_id}", "select": "*"},
    )
    return data[0] if data else None


# ---------------------------------------------------------------------------
# Seen attacks (per-guild)
# ---------------------------------------------------------------------------

async def is_attack_seen(guild_id: int, attack_id: int) -> bool:
    data = await _request(
        "GET",
        "seen_attacks",
        params={
            "guild_id": f"eq.{guild_id}",
            "attack_id": f"eq.{attack_id}",
            "select": "attack_id",
        },
    )
    return bool(data)


async def mark_attack_seen(guild_id: int, attack_id: int) -> None:
    await _request(
        "POST",
        "seen_attacks",
        params={"on_conflict": "guild_id,attack_id"},
        json_body={"guild_id": guild_id, "attack_id": attack_id},
        prefer="resolution=merge-duplicates,return=minimal",
    )


# ---------------------------------------------------------------------------
# War rooms
# ---------------------------------------------------------------------------

async def get_war_room(guild_id: int, enemy_nation_id: int) -> dict | None:
    data = await _request(
        "GET",
        "war_rooms",
        params={
            "guild_id": f"eq.{guild_id}",
            "enemy_nation_id": f"eq.{enemy_nation_id}",
            "select": "*",
        },
    )
    return data[0] if data else None


async def get_war_room_by_channel(channel_id: int) -> dict | None:
    data = await _request(
        "GET",
        "war_rooms",
        params={"channel_id": f"eq.{channel_id}", "select": "*"},
    )
    return data[0] if data else None


async def create_war_room(
    guild_id: int, enemy_nation_id: int, channel_id: int, category_id: int
) -> None:
    await _request(
        "POST",
        "war_rooms",
        params={"on_conflict": "guild_id,enemy_nation_id"},
        json_body={
            "guild_id": guild_id,
            "enemy_nation_id": enemy_nation_id,
            "channel_id": channel_id,
            "category_id": category_id,
        },
        prefer="resolution=merge-duplicates,return=minimal",
    )


async def get_all_war_rooms(guild_id: int) -> list[dict]:
    data = await _request(
        "GET",
        "war_rooms",
        params={"guild_id": f"eq.{guild_id}", "select": "*"},
    )
    return data or []


async def set_pin_message(
    guild_id: int, enemy_nation_id: int, message_id: int
) -> None:
    await _request(
        "PATCH",
        "war_rooms",
        params={
            "guild_id": f"eq.{guild_id}",
            "enemy_nation_id": f"eq.{enemy_nation_id}",
        },
        json_body={"pin_message_id": message_id},
        prefer="return=minimal",
    )


async def set_war_room_objective(
    guild_id: int, enemy_nation_id: int, objective: str
) -> None:
    await _request(
        "PATCH",
        "war_rooms",
        params={
            "guild_id": f"eq.{guild_id}",
            "enemy_nation_id": f"eq.{enemy_nation_id}",
        },
        json_body={"objective": objective},
        prefer="return=minimal",
    )


async def get_war_room_objective(
    guild_id: int, enemy_nation_id: int
) -> str | None:
    room = await get_war_room(guild_id, enemy_nation_id)
    return (room.get("objective") or None) if room else None


async def set_war_room_orders(
    guild_id: int,
    enemy_nation_id: int,
    orders_text: str,
    assignments: dict | None = None,
) -> None:
    """Store last orders. assignments is always a real dict (never a JSON string)."""
    await _request(
        "PATCH",
        "war_rooms",
        params={
            "guild_id": f"eq.{guild_id}",
            "enemy_nation_id": f"eq.{enemy_nation_id}",
        },
        json_body={
            "last_orders": orders_text[:8000],
            "order_assignments": assignments or {},
        },
        prefer="return=minimal",
    )


async def get_war_room_orders(guild_id: int, enemy_nation_id: int) -> dict:
    room = await get_war_room(guild_id, enemy_nation_id)
    if not room:
        return {"text": "", "assignments": {}, "objective": None}

    raw = room.get("order_assignments") or {}
    if isinstance(raw, str):
        try:
            assign = json.loads(raw)
        except Exception:
            assign = {}
    else:
        assign = raw if isinstance(raw, dict) else {}

    return {
        "text": room.get("last_orders") or "",
        "assignments": assign,
        "objective": room.get("objective"),
    }


async def schedule_war_room_delete(
    guild_id: int, enemy_nation_id: int, delete_at_iso: str | None
) -> None:
    await _request(
        "PATCH",
        "war_rooms",
        params={
            "guild_id": f"eq.{guild_id}",
            "enemy_nation_id": f"eq.{enemy_nation_id}",
        },
        json_body={"delete_at": delete_at_iso},
        prefer="return=minimal",
    )


async def list_war_rooms_due_delete() -> list[dict]:
    """Return rooms whose delete_at <= now (UTC)."""
    now = datetime.now(timezone.utc).isoformat()
    data = await _request(
        "GET",
        "war_rooms",
        params={
            "delete_at": f"lte.{now}",
            "select": "*",
        },
    )
    return data or []


# ---------------------------------------------------------------------------
# War room participants
# ---------------------------------------------------------------------------

async def add_room_participant(channel_id: int, discord_user_id: int) -> None:
    await _request(
        "POST",
        "war_room_participants",
        params={"on_conflict": "channel_id,discord_user_id"},
        json_body={"channel_id": channel_id, "discord_user_id": discord_user_id},
        prefer="resolution=merge-duplicates,return=minimal",
    )


async def remove_room_participant(channel_id: int, discord_user_id: int) -> None:
    await _request(
        "DELETE",
        "war_room_participants",
        params={
            "channel_id": f"eq.{channel_id}",
            "discord_user_id": f"eq.{discord_user_id}",
        },
        prefer="return=minimal",
    )


async def is_room_participant(channel_id: int, discord_user_id: int) -> bool:
    data = await _request(
        "GET",
        "war_room_participants",
        params={
            "channel_id": f"eq.{channel_id}",
            "discord_user_id": f"eq.{discord_user_id}",
            "select": "discord_user_id",
        },
    )
    return bool(data)


async def list_room_participants(channel_id: int) -> list[int]:
    data = await _request(
        "GET",
        "war_room_participants",
        params={
            "channel_id": f"eq.{channel_id}",
            "select": "discord_user_id",
        },
    )
    return [row["discord_user_id"] for row in (data or [])]
