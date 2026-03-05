"""
transform_and_load.py
=====================
ETL pipeline that reads raw_matches.json, flattens the nested Valorant match
payloads into three relational record sets, and loads them into PostgreSQL.

Tables:
    - matches            (1 row per match)
    - player_match_stats (10 rows per match — 1 per player)
    - kill_events        (variable rows per match)

Usage:
    # Dry run — prints transformed records to stdout, no DB required
    python transform_and_load.py --dry-run

    # Full load into PostgreSQL (requires env vars or .env)
    python transform_and_load.py

Environment Variables (for DB mode):
    DB_HOST     — PostgreSQL host      (default: localhost)
    DB_PORT     — PostgreSQL port      (default: 5432)
    DB_NAME     — Database name        (default: valorant_analytics)
    DB_USER     — Database user        (default: postgres)
    DB_PASSWORD — Database password    (default: "")
"""

import json
import os
import sys
import argparse
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# EXTRACT — Load raw JSON
# ---------------------------------------------------------------------------

def extract(filepath: str = "raw_matches.json") -> list[dict]:
    """Load raw match payloads from JSON file."""
    if not os.path.exists(filepath):
        print(f"❌ Error: {filepath} not found. Run generate_mock_data.py first.")
        sys.exit(1)

    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"📥 Extracted {len(data)} match(es) from {filepath}")
    return data


# ---------------------------------------------------------------------------
# TRANSFORM — Flatten nested structures into relational records
# ---------------------------------------------------------------------------

def transform_match(raw_match: dict) -> dict:
    """
    Transform a single raw match payload into a flat match record.

    Extracts: match_id, map, mode, timing, scores, region info.
    """
    meta = raw_match["metadata"]
    teams = raw_match["teams"]

    # Convert epoch millis → datetime
    game_start_epoch = meta["game_start"]
    game_start_dt = datetime.fromtimestamp(
        game_start_epoch / 1000, tz=timezone.utc
    )

    winning_team = "Blue" if teams["blue"]["has_won"] else "Red"

    return {
        "match_id":        meta["match_id"],
        "map_name":        meta["map"]["name"],
        "game_mode":       meta["mode"],
        "game_start":      game_start_dt.isoformat(),
        "game_length_ms":  meta["game_length_millis"],
        "rounds_played":   meta["rounds_played"],
        "winning_team":    winning_team,
        "region":          meta.get("region"),
        "cluster":         meta.get("cluster"),
        "game_version":    meta.get("game_version"),
        "season_id":       meta.get("season_id"),
        "blue_rounds_won": teams["blue"]["rounds_won"],
        "red_rounds_won":  teams["red"]["rounds_won"],
    }


def transform_players(raw_match: dict) -> list[dict]:
    """
    Flatten all player records from a match into relational rows.

    Each player's nested stats, ability_casts, and economy blocks
    are extracted and normalized.
    """
    match_id = raw_match["metadata"]["match_id"]
    players = raw_match["players"]["all_players"]
    records = []

    for player in players:
        stats = player.get("stats", {})
        record = {
            "match_id":        match_id,
            "puuid":           player["puuid"],
            "game_name":       player["game_name"],
            "tag_line":        player["tag_line"],
            "team":            player["team"],
            "agent":           player["character"],
            "kills":           stats.get("kills", 0),
            "deaths":          stats.get("deaths", 0),
            "assists":         stats.get("assists", 0),
            "score":           stats.get("score", 0),
            "headshots":       stats.get("headshots", 0),
            "bodyshots":       stats.get("bodyshots", 0),
            "legshots":        stats.get("legshots", 0),
            "damage_made":     player.get("damage_made", 0),
            "damage_received": player.get("damage_received", 0),
            # JSONB columns — keep as dicts, serialize on insert
            "ability_casts":   player.get("ability_casts"),
            "economy":         player.get("economy"),
        }
        records.append(record)

    return records


def transform_kill_events(raw_match: dict) -> list[dict]:
    """
    Flatten all kill events from a match into relational rows.

    Extracts killer/victim PUUIDs, spatial coordinates, weapon info,
    and serializes the assistants array for JSONB storage.
    """
    match_id = raw_match["metadata"]["match_id"]
    events = raw_match.get("kill_events", [])
    records = []

    for event in events:
        victim_loc = event.get("victim_death_location", {})

        # Try to find killer location from player_locations_on_kill
        killer_x = None
        killer_y = None
        player_locs = event.get("player_locations_on_kill", [])
        for loc in player_locs:
            if loc.get("player_puuid") == event.get("killer_puuid"):
                killer_x = loc["location"]["x"]
                killer_y = loc["location"]["y"]
                break

        # Extract assistant PUUIDs
        assistants = [
            a["assistant_puuid"]
            for a in event.get("assistants", [])
        ]

        record = {
            "match_id":            match_id,
            "round":               event["round"],
            "kill_time_in_round":  event["kill_time_in_round_in_millis"],
            "kill_time_in_match":  event["kill_time_in_match_in_millis"],
            "killer_puuid":        event["killer_puuid"],
            "victim_puuid":        event["victim_puuid"],
            "weapon":              event.get("damage_weapon_name") or None,
            "damage_type":         event["damage_type"],
            "killer_x":            killer_x,
            "killer_y":            killer_y,
            "victim_x":            victim_loc.get("x"),
            "victim_y":            victim_loc.get("y"),
            "assistants":          assistants,
            "finishing_damage":    event.get("finishing_damage"),
        }
        records.append(record)

    return records


def transform_all(raw_data: list[dict]) -> tuple[list, list, list]:
    """
    Full transformation pipeline across all matches.

    Returns:
        (match_records, player_records, kill_records)
    """
    all_matches = []
    all_players = []
    all_kills = []

    for raw_match in raw_data:
        all_matches.append(transform_match(raw_match))
        all_players.extend(transform_players(raw_match))
        all_kills.extend(transform_kill_events(raw_match))

    print(f"🔄 Transformed: {len(all_matches)} matches | "
          f"{len(all_players)} player stats | {len(all_kills)} kill events")

    return all_matches, all_players, all_kills


# ---------------------------------------------------------------------------
# LOAD — Insert into PostgreSQL
# ---------------------------------------------------------------------------

def _get_db_connection():
    """Create a PostgreSQL connection from environment variables."""
    try:
        import psycopg2
    except ImportError:
        print("❌ psycopg2 is not installed. Install with:")
        print("   pip install psycopg2-binary")
        sys.exit(1)

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "valorant_analytics"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
    )
    return conn


def load_matches(cursor, matches: list[dict]) -> None:
    """Insert match records into the matches table."""
    sql = """
        INSERT INTO matches (
            match_id, map_name, game_mode, game_start, game_length_ms,
            rounds_played, winning_team, region, cluster, game_version,
            season_id, blue_rounds_won, red_rounds_won
        ) VALUES (
            %(match_id)s, %(map_name)s, %(game_mode)s, %(game_start)s,
            %(game_length_ms)s, %(rounds_played)s, %(winning_team)s,
            %(region)s, %(cluster)s, %(game_version)s, %(season_id)s,
            %(blue_rounds_won)s, %(red_rounds_won)s
        )
        ON CONFLICT (match_id) DO NOTHING;
    """
    for record in matches:
        cursor.execute(sql, record)


def load_players(cursor, players: list[dict]) -> None:
    """Insert player stat records into player_match_stats table."""
    sql = """
        INSERT INTO player_match_stats (
            match_id, puuid, game_name, tag_line, team, agent,
            kills, deaths, assists, score,
            headshots, bodyshots, legshots,
            damage_made, damage_received,
            ability_casts, economy
        ) VALUES (
            %(match_id)s, %(puuid)s, %(game_name)s, %(tag_line)s,
            %(team)s, %(agent)s,
            %(kills)s, %(deaths)s, %(assists)s, %(score)s,
            %(headshots)s, %(bodyshots)s, %(legshots)s,
            %(damage_made)s, %(damage_received)s,
            %(ability_casts)s, %(economy)s
        );
    """
    for record in players:
        # Serialize JSONB fields
        record = dict(record)
        record["ability_casts"] = json.dumps(record["ability_casts"])
        record["economy"] = json.dumps(record["economy"])
        cursor.execute(sql, record)


def load_kill_events(cursor, kills: list[dict]) -> None:
    """Insert kill event records into kill_events table."""
    sql = """
        INSERT INTO kill_events (
            match_id, round, kill_time_in_round, kill_time_in_match,
            killer_puuid, victim_puuid, weapon, damage_type,
            killer_x, killer_y, victim_x, victim_y,
            assistants, finishing_damage
        ) VALUES (
            %(match_id)s, %(round)s, %(kill_time_in_round)s,
            %(kill_time_in_match)s, %(killer_puuid)s, %(victim_puuid)s,
            %(weapon)s, %(damage_type)s,
            %(killer_x)s, %(killer_y)s, %(victim_x)s, %(victim_y)s,
            %(assistants)s, %(finishing_damage)s
        );
    """
    for record in kills:
        record = dict(record)
        record["assistants"] = json.dumps(record["assistants"])
        record["finishing_damage"] = json.dumps(record["finishing_damage"])
        cursor.execute(sql, record)


def load_to_db(
    matches: list[dict],
    players: list[dict],
    kills: list[dict],
) -> None:
    """Execute the full database load."""
    conn = _get_db_connection()
    cursor = conn.cursor()

    try:
        print("📤 Loading data into PostgreSQL...")
        load_matches(cursor, matches)
        load_players(cursor, players)
        load_kill_events(cursor, kills)
        conn.commit()
        print("✅ Successfully loaded all records into the database.")
    except Exception as e:
        conn.rollback()
        print(f"❌ Database load failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


# ---------------------------------------------------------------------------
# DRY RUN — Pretty-print transformed records
# ---------------------------------------------------------------------------

def dry_run(
    matches: list[dict],
    players: list[dict],
    kills: list[dict],
) -> None:
    """Print a summary + sample records for each table to stdout."""
    separator = "=" * 72

    print(f"\n{separator}")
    print("  DRY RUN — Transformed Data Preview")
    print(f"{separator}\n")

    # --- Matches ---
    print(f"📋 MATCHES ({len(matches)} records)")
    print("-" * 40)
    for m in matches[:2]:
        print(f"  Match: {m['match_id'][:8]}...")
        print(f"    Map: {m['map_name']} | Mode: {m['game_mode']}")
        print(f"    Score: Blue {m['blue_rounds_won']} — Red {m['red_rounds_won']}")
        print(f"    Winner: {m['winning_team']} | Rounds: {m['rounds_played']}")
        print()

    if len(matches) > 2:
        print(f"  ... and {len(matches) - 2} more match(es)\n")

    # --- Players ---
    print(f"👤 PLAYER STATS ({len(players)} records)")
    print("-" * 40)
    for p in players[:3]:
        kda = f"{p['kills']}/{p['deaths']}/{p['assists']}"
        hs_pct = (
            f"{p['headshots'] / max(p['kills'], 1) * 100:.0f}%"
            if p["kills"] > 0 else "N/A"
        )
        print(f"  {p['game_name']}#{p['tag_line']} — {p['agent']} ({p['team']})")
        print(f"    KDA: {kda} | Score: {p['score']} | HS%: {hs_pct}")
        print()

    if len(players) > 3:
        print(f"  ... and {len(players) - 3} more player(s)\n")

    # --- Kill Events ---
    print(f"💀 KILL EVENTS ({len(kills)} records)")
    print("-" * 40)
    for k in kills[:3]:
        weapon = k["weapon"] or k["damage_type"]
        assists = len(k["assistants"]) if k["assistants"] else 0
        print(f"  Round {k['round']} @ {k['kill_time_in_round']}ms")
        print(f"    Weapon: {weapon} | Type: {k['damage_type']}")
        print(f"    Victim XY: ({k['victim_x']}, {k['victim_y']})")
        if k.get("killer_x") is not None:
            print(f"    Killer XY: ({k['killer_x']}, {k['killer_y']})")
        print(f"    Assistants: {assists}")
        print()

    if len(kills) > 3:
        print(f"  ... and {len(kills) - 3} more kill event(s)\n")

    print(f"{separator}")
    print("  ✅ Dry run complete — no data was written to the database.")
    print(f"{separator}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Valorant Match ETL — Transform & Load Pipeline"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print transformed records to stdout instead of loading to DB",
    )
    parser.add_argument(
        "--input",
        default="raw_matches.json",
        help="Path to raw match JSON file (default: raw_matches.json)",
    )
    args = parser.parse_args()

    # EXTRACT
    raw_data = extract(args.input)

    # TRANSFORM
    matches, players, kills = transform_all(raw_data)

    # LOAD
    if args.dry_run:
        dry_run(matches, players, kills)
    else:
        load_to_db(matches, players, kills)


if __name__ == "__main__":
    main()
