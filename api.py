"""
api.py
======
FastAPI REST API for the Valorant Esports Match Analytics Platform.

Provides three read-heavy GET endpoints backed by an asyncpg connection
pool to PostgreSQL for maximum throughput.

Endpoints:
    GET /api/matches                        — List all matches
    GET /api/matches/{match_id}/stats       — Player leaderboard for a match
    GET /api/matches/{match_id}/heatmaps    — X/Y kill coordinate data

Usage:
    uvicorn api:app --reload --port 8000

Environment Variables (loaded from .env via python-dotenv):
    DB_HOST     — PostgreSQL host      (required)
    DB_PORT     — PostgreSQL port      (default: 5432)
    DB_NAME     — Database name        (required)
    DB_USER     — Database user        (required)
    DB_PASSWORD — Database password    (required)
"""

import os
import sys
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# ---------------------------------------------------------------------------
# Configuration — load .env and validate
# ---------------------------------------------------------------------------

load_dotenv()

REQUIRED_ENV_VARS = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"]


def _validate_env():
    """
    Validate that all required database environment variables are set.

    Raises a clear, readable error listing every missing variable so
    the developer knows exactly what to add to their .env file.
    """
    missing = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]
    if missing:
        print("\n" + "=" * 60)
        print("  ❌ MISSING ENVIRONMENT VARIABLES")
        print("=" * 60)
        for var in missing:
            print(f"  • {var}")
        print()
        print("  Create a .env file in the project root with:")
        print()
        print('    DB_HOST=your_database_host')
        print('    DB_NAME=your_database_name')
        print('    DB_USER=your_database_user')
        print('    DB_PASSWORD=your_database_password')
        print("=" * 60 + "\n")
        sys.exit(1)


_validate_env()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME"),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# ---------------------------------------------------------------------------
# Database connection pool (async)
# ---------------------------------------------------------------------------

# Module-level pool reference, managed by the lifespan context
pool: Optional[asyncpg.Pool] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage the asyncpg connection pool lifecycle.

    The pool is created on startup and closed on shutdown, avoiding
    per-request connection overhead on read-heavy workloads.
    """
    global pool
    try:
        pool = await asyncpg.create_pool(
            **DB_CONFIG,
            min_size=2,
            max_size=10,
            command_timeout=30,
        )
        print(f"✅ Database pool created → {DB_CONFIG['database']}@{DB_CONFIG['host']}")
    except Exception as e:
        print(f"⚠️  Database pool creation failed: {e}")
        print("   API will start but DB endpoints will return 503.")
        pool = None
    yield
    if pool:
        await pool.close()
        print("🔌 Database pool closed.")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Valorant Analytics API",
    description=(
        "REST API for querying Valorant esports match data — "
        "match metadata, player leaderboards, and kill heatmap coordinates."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# Allow frontend origins (restrict in production to specific domains)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ensure_pool():
    """Raise 503 if the database pool is not available."""
    if pool is None:
        raise HTTPException(
            status_code=503,
            detail="Database connection is not available. Check DB_* env vars.",
        )


def _rows_to_dicts(rows: list[asyncpg.Record]) -> list[dict]:
    """Convert asyncpg Record objects to plain dicts (JSON-serializable)."""
    result = []
    for row in rows:
        d = dict(row)
        # Convert any non-serializable types
        for key, value in d.items():
            if isinstance(value, (dict, list)):
                continue  # already JSON-friendly
            if hasattr(value, "isoformat"):
                d[key] = value.isoformat()
        result.append(d)
    return result


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/", tags=["Health"])
async def root():
    """API health check and welcome message."""
    return {
        "service": "Valorant Analytics API",
        "version": "1.0.0",
        "status": "operational",
        "docs": "/docs",
    }


@app.get("/health", tags=["Health"])
async def health_check():
    """Detailed health check including database connectivity."""
    db_status = "disconnected"
    if pool:
        try:
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            db_status = "connected"
        except Exception:
            db_status = "error"
    return {
        "api": "healthy",
        "database": db_status,
    }


# ---------------------------------------------------------------------------
# Endpoint 1: GET /api/matches
# ---------------------------------------------------------------------------

@app.get("/api/matches", tags=["Matches"])
async def get_matches(
    limit: int = Query(default=50, ge=1, le=200, description="Max results"),
    offset: int = Query(default=0, ge=0, description="Pagination offset"),
    map_name: Optional[str] = Query(default=None, description="Filter by map"),
    game_mode: Optional[str] = Query(default=None, description="Filter by mode"),
):
    """
    Return a list of all matches with high-level metadata.

    Supports pagination via `limit`/`offset` and optional filtering
    by `map_name` and `game_mode`.
    """
    _ensure_pool()

    # Build dynamic WHERE clause
    conditions = []
    params = []
    idx = 1

    if map_name:
        conditions.append(f"map_name = ${idx}")
        params.append(map_name)
        idx += 1
    if game_mode:
        conditions.append(f"game_mode = ${idx}")
        params.append(game_mode)
        idx += 1

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    query = f"""
        SELECT
            match_id, map_name, game_mode, game_start, game_length_ms,
            rounds_played, winning_team, region, cluster,
            blue_rounds_won, red_rounds_won
        FROM matches
        {where_clause}
        ORDER BY game_start DESC
        LIMIT ${idx} OFFSET ${idx + 1}
    """
    params.extend([limit, offset])

    # Count query for pagination metadata
    count_query = f"SELECT COUNT(*) FROM matches {where_clause}"
    count_params = params[:-2]  # exclude limit/offset

    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)
        total = await conn.fetchval(count_query, *count_params)

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "matches": _rows_to_dicts(rows),
    }


# ---------------------------------------------------------------------------
# Endpoint 2: GET /api/matches/{match_id}/stats
# ---------------------------------------------------------------------------

@app.get("/api/matches/{match_id}/stats", tags=["Matches"])
async def get_match_stats(match_id: str):
    """
    Return the full player leaderboard for a specific match.

    Includes kills, deaths, assists, score, headshot %, economy,
    ability casts, and damage dealt/received — sorted by score descending.
    """
    _ensure_pool()

    async with pool.acquire() as conn:
        # Verify match exists
        match_row = await conn.fetchrow(
            """
            SELECT match_id, map_name, game_mode, game_start, rounds_played,
                   winning_team, blue_rounds_won, red_rounds_won
            FROM matches
            WHERE match_id = $1
            """,
            match_id,
        )
        if not match_row:
            raise HTTPException(status_code=404, detail=f"Match {match_id} not found")

        # Fetch player stats sorted by score
        player_rows = await conn.fetch(
            """
            SELECT
                puuid, game_name, tag_line, team, agent,
                kills, deaths, assists, score,
                headshots, bodyshots, legshots,
                damage_made, damage_received,
                ability_casts, economy,
                CASE WHEN kills > 0
                     THEN ROUND(headshots::numeric / kills * 100, 1)
                     ELSE 0
                END AS headshot_pct,
                CASE WHEN deaths > 0
                     THEN ROUND(kills::numeric / deaths, 2)
                     ELSE kills::numeric
                END AS kd_ratio
            FROM player_match_stats
            WHERE match_id = $1
            ORDER BY score DESC
            """,
            match_id,
        )

    match_meta = dict(match_row)
    if hasattr(match_meta.get("game_start"), "isoformat"):
        match_meta["game_start"] = match_meta["game_start"].isoformat()

    players = _rows_to_dicts(player_rows)

    return {
        "match": match_meta,
        "player_count": len(players),
        "leaderboard": players,
    }


# ---------------------------------------------------------------------------
# Endpoint 3: GET /api/matches/{match_id}/heatmaps
# ---------------------------------------------------------------------------

@app.get("/api/matches/{match_id}/heatmaps", tags=["Matches"])
async def get_match_heatmaps(
    match_id: str,
    round_num: Optional[int] = Query(default=None, description="Filter by round"),
    weapon: Optional[str] = Query(default=None, description="Filter by weapon"),
):
    """
    Return granular X/Y coordinate data for kill events in a match.

    Each record includes killer and victim positions, weapon used,
    round number, and timing — designed for frontend heatmap rendering.

    Supports optional filtering by `round_num` and `weapon`.
    """
    _ensure_pool()

    async with pool.acquire() as conn:
        # Verify match exists
        match_exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM matches WHERE match_id = $1)",
            match_id,
        )
        if not match_exists:
            raise HTTPException(status_code=404, detail=f"Match {match_id} not found")

        # Build dynamic query
        conditions = ["match_id = $1"]
        params = [match_id]
        idx = 2

        if round_num is not None:
            conditions.append(f"round = ${idx}")
            params.append(round_num)
            idx += 1
        if weapon:
            conditions.append(f"weapon = ${idx}")
            params.append(weapon)
            idx += 1

        where_clause = " AND ".join(conditions)

        kill_rows = await conn.fetch(
            f"""
            SELECT
                round,
                kill_time_in_round,
                kill_time_in_match,
                killer_puuid,
                victim_puuid,
                weapon,
                damage_type,
                killer_x,
                killer_y,
                victim_x,
                victim_y,
                assistants
            FROM kill_events
            WHERE {where_clause}
            ORDER BY kill_time_in_match ASC
            """,
            *params,
        )

        # Fetch map name for coordinate context
        map_name = await conn.fetchval(
            "SELECT map_name FROM matches WHERE match_id = $1",
            match_id,
        )

    kills = _rows_to_dicts(kill_rows)

    # Compute summary stats for the frontend
    victim_coords = [
        {"x": k["victim_x"], "y": k["victim_y"]}
        for k in kills if k.get("victim_x") is not None
    ]
    killer_coords = [
        {"x": k["killer_x"], "y": k["killer_y"]}
        for k in kills if k.get("killer_x") is not None
    ]

    return {
        "match_id": match_id,
        "map_name": map_name,
        "total_kills": len(kills),
        "filters_applied": {
            "round": round_num,
            "weapon": weapon,
        },
        "summary": {
            "victim_positions_count": len(victim_coords),
            "killer_positions_count": len(killer_coords),
        },
        "kill_events": kills,
    }


# ---------------------------------------------------------------------------
# Run with: uvicorn api:app --reload --port 8000
# ---------------------------------------------------------------------------
