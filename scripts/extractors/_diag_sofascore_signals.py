"""Diagnostico Sofascore (senales de fatiga + bajas): 1 partido / 1 equipo.

Confirma DOS formas de JSON que no podemos ver offline, antes de escribir parsers:
  (1) que '/event/{id}/lineups' trae 'missingPlayers' con el motivo (baja por
      lesion/sancion/duda) -> senal "jugadores que se pierden el partido".
  (2) que '/team/{teamId}/events/last|next' devuelve TODAS las competiciones
      (Liga+Copa+Europa+Supercopa+Mundial) -> base para dias de descanso reales,
      congestion, paron de selecciones y viaje europeo.

COSTE: ~5 requests. Reutiliza el transporte de sofascore.py (curl_cffi en cascada).
EJECUTAR con el .venv (curl_cffi instalado):
    python scripts\\extractors\\_diag_sofascore_signals.py
"""
from __future__ import annotations

import sys
import pathlib
from datetime import datetime, timezone

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import sofascore as sofa  # reutiliza SofaClient + _fetch_json

SEED_MATCH_ID = 14083739  # Valencia - FC Barcelona, 2026-05-23 (finished)


def _get(session, path):
    """Devuelve el payload real (desenvuelve el sobre {ok,data,err})."""
    try:
        res = sofa._fetch_json(session, path)
    except Exception as e:
        print(f"   [ERR] {path} -> {type(e).__name__}: {e}")
        return None
    if isinstance(res, dict) and "data" in res and set(res.keys()) <= {"ok", "data", "err"}:
        if not res.get("ok"):
            print(f"   [ERR] {path} -> {res.get('err')}")
            return None
        return res.get("data")
    return res


def _dump_keys(label, obj, n=25):
    if isinstance(obj, dict):
        print(f"   {label} keys: {list(obj.keys())[:n]}")
    elif isinstance(obj, list):
        print(f"   {label}: lista de {len(obj)}; primer item keys: "
              f"{list(obj[0].keys())[:n] if obj and isinstance(obj[0], dict) else obj[:3]}")


def probe_lineups(session, mid, tag):
    print(f"\n--- LINEUPS de {tag} (event {mid}) ---")
    lu = _get(session, f"/api/v1/event/{mid}/lineups")
    if not lu:
        return
    print(f"   confirmed={lu.get('confirmed')}  keys={list(lu.keys())}")
    for side in ("home", "away"):
        blk = lu.get(side) or {}
        miss = blk.get("missingPlayers")
        if miss is None:
            print(f"   {side}: NO hay 'missingPlayers'")
            continue
        print(f"   {side}: missingPlayers = {len(miss)}")
        for it in miss:
            p = it.get("player") or {}
            print(f"      - {p.get('name'):<22} "
                  f"type={it.get('type')!r} reason={it.get('reason')!r} "
                  f"desc={it.get('description')!r} ext={it.get('externalType')!r} "
                  f"end={it.get('expectedEndDate')!r}")


def probe_team_calendar(session, team_id, team_name):
    print(f"\n--- CALENDARIO multi-competicion de {team_name} (team {team_id}) ---")
    comps = {}
    events = []
    for direction in ("last", "next"):
        js = _get(session, f"/api/v1/team/{team_id}/events/{direction}/0")
        evs = (js or {}).get("events", []) if isinstance(js, dict) else []
        print(f"   events/{direction}/0 -> {len(evs)} partidos")
        for e in evs:
            ut = ((e.get("tournament") or {}).get("uniqueTournament") or {})
            name = ut.get("name") or (e.get("tournament") or {}).get("name")
            comps[name] = comps.get(name, 0) + 1
            ts = e.get("startTimestamp")
            events.append((ts, name, e.get("homeTeam", {}).get("name"),
                           e.get("awayTeam", {}).get("name"), e.get("id")))
    print("   competiciones detectadas (recuento):")
    for k, v in sorted(comps.items(), key=lambda x: -x[1]):
        print(f"      {v:3d}  {k}")
    # muestra de dias de descanso entre los ultimos partidos jugados
    played = sorted([e for e in events if e[0]], key=lambda x: x[0])
    print("   ultimos partidos ordenados (dias desde el anterior):")
    prev = None
    for ts, name, h, a, eid in played[-8:]:
        d = datetime.fromtimestamp(ts, tz=timezone.utc)
        rest = f"{(ts - prev)/86400:.1f}d" if prev else "  -  "
        print(f"      {d:%Y-%m-%d}  rest={rest:>6}  {name:<28} {h} vs {a}")
        prev = ts
    return played


def main():
    session = sofa._new_session()
    try:
        # 1) evento -> ids de equipo + torneo
        ev = _get(session, f"/api/v1/event/{SEED_MATCH_ID}")
        e = (ev or {}).get("event", ev) if ev else None
        if not e:
            print("[STOP] no se pudo leer el evento semilla.")
            return
        home, away = e.get("homeTeam", {}), e.get("awayTeam", {})
        print(f"[INFO] semilla: {home.get('name')} (id={home.get('id')}) vs "
              f"{away.get('name')} (id={away.get('id')})")
        _dump_keys("event", e)

        # 2) missingPlayers en partido jugado
        probe_lineups(session, SEED_MATCH_ID, "partido jugado")

        # 3) calendario multi-competicion del local + dias de descanso
        played = probe_team_calendar(session, home.get("id"), home.get("name"))

        # 4) missingPlayers en un partido PROXIMO (suele ser donde se puebla)
        upcoming = [p for p in played if p[0] and p[0] > datetime.now(timezone.utc).timestamp()]
        if upcoming:
            probe_lineups(session, upcoming[0][4], "partido proximo")
        else:
            print("\n[INFO] sin partidos proximos en el calendario (temporada cerrada);"
                  " no se pudo probar missingPlayers pre-partido.")

        print("\n=== FIN DIAGNOSTICO ===")
        print("Pega la salida completa. Fijate en: (a) si aparece 'missingPlayers' y con "
              "que campos; (b) la lista de competiciones detectadas.")
    finally:
        try:
            session.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
