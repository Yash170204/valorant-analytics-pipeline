"""
generate_mock_data.py
=====================
Generates realistic mock Valorant match JSON payloads that mimic the
HenrikDev Valorant API (v3 match history) response structure.

Outputs: raw_matches.json (5 complete match payloads)

Usage:
    python generate_mock_data.py
"""

import json
import random
import uuid
import string
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# Constants — Authentic Valorant game data
# ---------------------------------------------------------------------------

MAPS = ["Ascent", "Bind", "Haven", "Split", "Icebox", "Breeze", "Fracture",
        "Pearl", "Lotus", "Sunset", "Abyss"]

AGENTS = [
    # Duelists
    "Jett", "Reyna", "Raze", "Phoenix", "Yoru", "Neon", "Iso",
    # Initiators
    "Sova", "Breach", "Skye", "KAY/O", "Fade", "Gekko",
    # Controllers
    "Brimstone", "Omen", "Viper", "Astra", "Harbor", "Clove",
    # Sentinels
    "Sage", "Cypher", "Killjoy", "Chamber", "Deadlock", "Vyse",
]

WEAPONS = [
    # Sidearms
    "Classic", "Shorty", "Frenzy", "Ghost", "Sheriff",
    # SMGs
    "Stinger", "Spectre",
    # Rifles
    "Bulldog", "Guardian", "Phantom", "Vandal",
    # Sniper Rifles
    "Marshal", "Outlaw", "Operator",
    # Shotguns
    "Bucky", "Judge",
    # Machine Guns
    "Ares", "Odin",
    # Melee
    "Melee",
]

DAMAGE_TYPES = ["Weapon", "Bomb", "Ability", "Melee", "Fall"]

FINISHING_DAMAGE_TYPES = [
    "Standard", "Headshot", "Wallbang", "Blind Kill", "Penetration Kill",
]

GAME_MODES = ["Competitive", "Unrated", "Spike Rush", "Deathmatch"]

# Map coordinate bounds (approximate in-game units)
MAP_BOUNDS = {
    "Ascent":   {"x": (-6000, 8000),  "y": (-6000, 8000)},
    "Bind":     {"x": (-5500, 7500),  "y": (-5500, 7500)},
    "Haven":    {"x": (-6000, 9000),  "y": (-6000, 9000)},
    "Split":    {"x": (-6000, 8000),  "y": (-6000, 8000)},
    "Icebox":   {"x": (-5000, 7000),  "y": (-5000, 7000)},
    "Breeze":   {"x": (-7000, 9000),  "y": (-7000, 9000)},
    "Fracture": {"x": (-6500, 8500),  "y": (-6500, 8500)},
    "Pearl":    {"x": (-6000, 8000),  "y": (-6000, 8000)},
    "Lotus":    {"x": (-6000, 8500),  "y": (-6000, 8500)},
    "Sunset":   {"x": (-5500, 7500),  "y": (-5500, 7500)},
    "Abyss":    {"x": (-6000, 8000),  "y": (-6000, 8000)},
}

# Realistic Riot-style tag lines
TAG_LINE_POOL = [
    "NA1", "EUW", "KR1", "BR1", "AP1", "0001", "1337", "9999",
    "GOAT", "ACE", "RIOT", "VALO", "GG", "EZ",
]

# Realistic gamer names (pool to sample from)
GAMER_NAMES = [
    "TenZ", "Aspas", "Demon1", "ZmjjKK", "Chronicle", "Derke",
    "yay", "Cned", "ShahZaM", "SicK", "Boaster", "nAts",
    "Sayf", "Less", "Saadhak", "Alfa", "Marved", "FNS",
    "Ardiis", "f0rsakeN", "MaKo", "Buzz", "Stax", "Rb",
    "Crashies", "Victor", "Mako", "Dep", "Benkai", "Mindfreak",
    "Enzo", "Leo", "Shao", "Zyppan", "Jinggg", "SugarZ3ro",
    "Mazino", "BuZz", "Lakia", "Xeta", "trexx", "ScreaM",
    "Nivera", "L1NK", "StarXo", "BONECOLD", "Ange1", "ANGE1",
    "Redgar", "Suygetsu",
]


# ---------------------------------------------------------------------------
# Helper generators
# ---------------------------------------------------------------------------

def _generate_puuid() -> str:
    """Generate a realistic-looking PUUID (Riot-style hex UUID)."""
    return uuid.uuid4().hex + uuid.uuid4().hex[:32]


def _generate_match_id() -> str:
    """Generate a UUID-based match ID."""
    return str(uuid.uuid4())


def _random_coord(map_name: str, axis: str) -> float:
    """Return a random coordinate within realistic map bounds."""
    bounds = MAP_BOUNDS.get(map_name, {"x": (-6000, 8000), "y": (-6000, 8000)})
    low, high = bounds[axis]
    return round(random.uniform(low, high), 2)


def _random_tag_line() -> str:
    return random.choice(TAG_LINE_POOL)


def _pick_unique_names(n: int) -> list[str]:
    """Pick n unique gamer names, generating extras if the pool is too small."""
    pool = list(GAMER_NAMES)
    random.shuffle(pool)
    if n <= len(pool):
        return pool[:n]
    # Generate extra names to fill
    extras = [f"Player{random.randint(1000, 9999)}" for _ in range(n - len(pool))]
    return pool + extras


def _generate_ability_casts() -> dict:
    """Generate realistic ability cast counts."""
    return {
        "c_cast": random.randint(0, 8),
        "q_cast": random.randint(0, 10),
        "e_cast": random.randint(0, 15),
        "x_cast": random.randint(0, 3),
    }


def _generate_economy() -> dict:
    """Generate realistic round economy snapshot."""
    loadout = random.choice([800, 2900, 3900, 4400, 4700, 5000, 5200, 5400])
    spent = random.randint(0, loadout)
    remaining = loadout - spent
    return {
        "loadout_value": loadout,
        "spent": spent,
        "remaining": remaining,
    }


# ---------------------------------------------------------------------------
# Core generators
# ---------------------------------------------------------------------------

def generate_players(match_map: str) -> tuple[list[dict], list[str]]:
    """
    Generate 10 players (5 per team) with full stat blocks.

    Returns:
        (players_list, puuid_list)
    """
    names = _pick_unique_names(10)
    agents = random.sample(AGENTS, 10)
    teams = ["Blue"] * 5 + ["Red"] * 5
    players = []
    puuids = []

    for i in range(10):
        puuid = _generate_puuid()
        puuids.append(puuid)

        kills = random.randint(2, 32)
        deaths = random.randint(2, 28)
        assists = random.randint(0, 15)
        headshots = random.randint(0, kills)
        bodyshots = random.randint(0, kills * 2)
        legshots = random.randint(0, max(1, kills // 2))

        player = {
            "puuid": puuid,
            "game_name": names[i],
            "tag_line": _random_tag_line(),
            "team": teams[i],
            "character": agents[i],
            "currenttier": random.randint(3, 27),  # Iron 1 → Radiant
            "currenttier_patched": f"Rank {random.randint(3, 27)}",
            "player_card": str(uuid.uuid4()),
            "player_title": str(uuid.uuid4()),
            "party_id": str(uuid.uuid4()),
            "session_playtime": {
                "minutes": random.randint(10, 60),
                "seconds": random.randint(0, 59),
                "milliseconds": random.randint(0, 999),
            },
            "behavior": {
                "afk_rounds": random.choice([0, 0, 0, 0, 1]),
                "friendly_fire": {
                    "incoming": random.randint(0, 50),
                    "outgoing": random.randint(0, 30),
                },
                "rounds_in_spawn": random.choice([0, 0, 0, 1, 2]),
            },
            "platform": {
                "type": random.choice(["PC", "PC", "PC", "Console"]),
                "os": {
                    "name": "Windows",
                    "version": "10.0.19045.1.256.64bit",
                },
            },
            "ability_casts": _generate_ability_casts(),
            "assets": {
                "card": {
                    "small": f"https://media.valorant-api.com/playercards/{uuid.uuid4()}/smallart.png",
                    "large": f"https://media.valorant-api.com/playercards/{uuid.uuid4()}/largeart.png",
                    "wide": f"https://media.valorant-api.com/playercards/{uuid.uuid4()}/wideart.png",
                },
                "agent": {
                    "small": f"https://media.valorant-api.com/agents/{uuid.uuid4()}/displayicon.png",
                    "full": f"https://media.valorant-api.com/agents/{uuid.uuid4()}/fullportrait.png",
                    "bust": f"https://media.valorant-api.com/agents/{uuid.uuid4()}/bustportrait.png",
                    "killfeed": f"https://media.valorant-api.com/agents/{uuid.uuid4()}/killfeedportrait.png",
                },
            },
            "stats": {
                "score": kills * 200 + assists * 100 + random.randint(-500, 500),
                "kills": kills,
                "deaths": deaths,
                "assists": assists,
                "headshots": headshots,
                "bodyshots": bodyshots,
                "legshots": legshots,
            },
            "economy": _generate_economy(),
            "damage_made": random.randint(500, 5500),
            "damage_received": random.randint(400, 5000),
        }
        players.append(player)

    return players, puuids


def generate_kill_events(
    puuids: list[str],
    match_map: str,
    rounds_played: int,
) -> list[dict]:
    """
    Generate realistic kill events across all rounds of the match.

    Each round typically has 3-8 kills. Kill timestamps increase
    monotonically within a round and reset each round.
    """
    kill_events = []

    for round_num in range(1, rounds_played + 1):
        num_kills = random.randint(3, 8)
        # Round timer starts at a random offset, kills occur within the round
        round_start_ms = round_num * 120_000  # ~2 min per round
        kill_time = round_start_ms + random.randint(5_000, 20_000)

        for _ in range(num_kills):
            killer = random.choice(puuids)
            victim = random.choice([p for p in puuids if p != killer])
            assistants_pool = [p for p in puuids if p not in (killer, victim)]
            num_assistants = random.choices([0, 1, 2], weights=[50, 35, 15])[0]
            assistants = random.sample(
                assistants_pool, min(num_assistants, len(assistants_pool))
            )

            weapon = random.choice(WEAPONS)
            damage_type = "Weapon" if weapon != "Melee" else "Melee"
            if random.random() < 0.08:
                damage_type = random.choice(["Ability", "Bomb"])
                weapon = ""

            event = {
                "kill_time_in_round_in_millis": kill_time - round_start_ms,
                "kill_time_in_match_in_millis": kill_time,
                "round": round_num - 1,  # API uses 0-indexed rounds
                "killer_puuid": killer,
                "killer_display_name": "",  # filled by API, keep for schema compat
                "killer_team": "",
                "victim_puuid": victim,
                "victim_display_name": "",
                "victim_team": "",
                "victim_death_location": {
                    "x": _random_coord(match_map, "x"),
                    "y": _random_coord(match_map, "y"),
                },
                "damage_weapon_id": weapon,
                "damage_weapon_name": weapon,
                "damage_type": damage_type,
                "finishing_damage": {
                    "damage_type": damage_type,
                    "damage_item": weapon.lower().replace(" ", "") if weapon else "ability",
                    "is_secondary_fire_mode": random.choice([False, False, False, True]),
                },
                "assistants": [
                    {"assistant_puuid": a, "assistant_display_name": ""}
                    for a in assistants
                ],
                "player_locations_on_kill": [
                    {
                        "player_puuid": p,
                        "player_display_name": "",
                        "player_team": "",
                        "location": {
                            "x": _random_coord(match_map, "x"),
                            "y": _random_coord(match_map, "y"),
                        },
                        "view_radians": round(random.uniform(0, 6.28), 4),
                    }
                    for p in random.sample(puuids, random.randint(4, 10))
                ],
            }
            kill_events.append(event)
            kill_time += random.randint(2_000, 15_000)

    return kill_events


def generate_match() -> dict:
    """Generate a single complete match payload mimicking the HenrikDev API."""
    match_map = random.choice(MAPS)
    game_mode = random.choice(GAME_MODES)

    # Score: first team to 13 (or overtime up to 15)
    blue_rounds = random.randint(5, 15)
    red_rounds = random.randint(5, 15)
    # Ensure one team wins (at least 13) with realistic margin
    if random.random() < 0.5:
        blue_rounds = 13 + random.choice([0, 0, 0, 1, 2])
        red_rounds = random.randint(4, blue_rounds - 1)
    else:
        red_rounds = 13 + random.choice([0, 0, 0, 1, 2])
        blue_rounds = random.randint(4, red_rounds - 1)

    rounds_played = blue_rounds + red_rounds
    winning_team = "Blue" if blue_rounds > red_rounds else "Red"

    # Match timestamp — within the last 30 days
    game_start = datetime.now(timezone.utc) - timedelta(
        days=random.randint(0, 30),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )
    game_length_millis = rounds_played * random.randint(100_000, 140_000)

    match_id = _generate_match_id()
    players, puuids = generate_players(match_map)
    kill_events = generate_kill_events(puuids, match_map, rounds_played)

    return {
        "metadata": {
            "match_id": match_id,
            "map": {
                "id": f"/Game/Maps/{match_map}/{match_map}",
                "name": match_map,
            },
            "game_version": "release-09.08-shipping-28-2594039",
            "game_length_millis": game_length_millis,
            "game_start": int(game_start.timestamp() * 1000),
            "game_start_patched": game_start.strftime("%A, %B %d, %Y %I:%M %p"),
            "rounds_played": rounds_played,
            "mode": game_mode,
            "mode_id": game_mode.lower().replace(" ", ""),
            "queue": {
                "id": game_mode.lower().replace(" ", ""),
                "name": game_mode,
                "mode_type": "Standard" if game_mode != "Deathmatch" else "Deathmatch",
            },
            "season_id": str(uuid.uuid4()),
            "platform": "PC",
            "premier_info": {
                "tournament_id": None,
                "matchup_id": None,
            },
            "region": random.choice(["na", "eu", "ap", "kr", "br"]),
            "cluster": random.choice([
                "us-west-1", "us-east-1", "eu-west-1", "ap-southeast-1",
                "kr-1", "br-1",
            ]),
        },
        "players": {
            "all_players": players,
            "blue": [p for p in players if p["team"] == "Blue"],
            "red": [p for p in players if p["team"] == "Red"],
        },
        "teams": {
            "blue": {
                "has_won": winning_team == "Blue",
                "rounds_won": blue_rounds,
                "rounds_lost": red_rounds,
                "roster": None,
            },
            "red": {
                "has_won": winning_team == "Red",
                "rounds_won": red_rounds,
                "rounds_lost": blue_rounds,
                "roster": None,
            },
        },
        "rounds": [
            {
                "winning_team": random.choice(["Blue", "Red"]),
                "end_type": random.choice([
                    "Eliminated", "Bomb detonated", "Bomb defused",
                    "Eliminated", "Eliminated", "Round timer expired",
                ]),
                "bomb_planted": random.choice([True, False]),
                "bomb_defused": random.choice([True, False, False]),
                "plant_events": {
                    "plant_location": {
                        "x": _random_coord(match_map, "x"),
                        "y": _random_coord(match_map, "y"),
                    },
                    "planted_by": {
                        "puuid": random.choice(puuids),
                        "display_name": "",
                        "team": random.choice(["Blue", "Red"]),
                    },
                    "plant_site": random.choice(["A", "B", "C"]),
                    "plant_time_in_round_millis": random.randint(25_000, 80_000),
                } if random.random() > 0.35 else None,
                "defuse_events": None,
                "player_stats": [
                    {
                        "player_puuid": p,
                        "kills": random.randint(0, 4),
                        "damage": random.randint(0, 450),
                        "score": random.randint(0, 400),
                        "economy": _generate_economy(),
                        "was_afk": False,
                        "was_penalized": False,
                        "stayed_in_spawn": False,
                    }
                    for p in puuids
                ],
            }
            for _ in range(rounds_played)
        ],
        "kill_events": kill_events,
    }


def main():
    """Generate mock matches and write to raw_matches.json."""
    num_matches = 5
    print(f"🎮 Generating {num_matches} mock Valorant match payloads...")

    matches = [generate_match() for _ in range(num_matches)]

    output_file = "raw_matches.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(matches, f, indent=2, ensure_ascii=False)

    # Summary stats
    total_players = sum(len(m["players"]["all_players"]) for m in matches)
    total_kills = sum(len(m["kill_events"]) for m in matches)
    total_rounds = sum(m["metadata"]["rounds_played"] for m in matches)

    print(f"✅ Successfully generated {output_file}")
    print(f"   📊 {num_matches} matches | {total_players} player records | "
          f"{total_kills} kill events | {total_rounds} rounds")
    print(f"   📁 File size: {len(json.dumps(matches)):,} characters")


if __name__ == "__main__":
    main()
