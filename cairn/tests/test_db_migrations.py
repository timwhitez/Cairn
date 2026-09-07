from __future__ import annotations

import sqlite3

from cairn.server import db


def test_configure_adds_bootstrap_enabled_to_legacy_projects_table(tmp_path, monkeypatch) -> None:
    path = tmp_path / "legacy.db"
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE projects (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL,
                reason_worker TEXT,
                reason_trigger TEXT,
                reason_started_at TEXT,
                reason_last_heartbeat_at TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO projects (id, title, created_at) VALUES ('proj_001', 'legacy', '2026-01-01T00:00:00Z')"
        )

    monkeypatch.setattr(db, "_db_path", None)
    db.configure(path)

    with db.get_conn() as conn:
        row = conn.execute("SELECT bootstrap_enabled FROM projects WHERE id = 'proj_001'").fetchone()
    assert row["bootstrap_enabled"] == 1


def test_configure_maps_disabled_bootstrap_mode_to_false(tmp_path, monkeypatch) -> None:
    path = tmp_path / "intermediate.db"
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE projects (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                bootstrap_mode TEXT NOT NULL DEFAULT 'auto',
                created_at TEXT NOT NULL,
                reason_worker TEXT,
                reason_trigger TEXT,
                reason_started_at TEXT,
                reason_last_heartbeat_at TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO projects (id, title, bootstrap_mode, created_at) VALUES ('proj_001', 'disabled', 'disabled', '2026-01-01T00:00:00Z')"
        )
        conn.execute(
            "INSERT INTO projects (id, title, bootstrap_mode, created_at) VALUES ('proj_002', 'enabled', 'enabled', '2026-01-01T00:00:00Z')"
        )

    monkeypatch.setattr(db, "_db_path", None)
    db.configure(path)

    with db.get_conn() as conn:
        rows = conn.execute("SELECT id, bootstrap_enabled FROM projects ORDER BY id").fetchall()
    assert [(row["id"], row["bootstrap_enabled"]) for row in rows] == [
        ("proj_001", 0),
        ("proj_002", 1),
    ]


def test_configure_adds_and_backfills_project_started_at(tmp_path, monkeypatch) -> None:
    path = tmp_path / "legacy-started-at.db"
    with sqlite3.connect(path) as conn:
        conn.executescript(db.SCHEMA.replace("    started_at TEXT,\n", ""))
        conn.execute(
            "INSERT INTO projects (id, title, created_at) VALUES ('proj_001', 'legacy', '2026-01-01T00:00:00Z')"
        )
        conn.execute(
            "INSERT INTO intents (id, project_id, description, creator, created_at) "
            "VALUES ('i001', 'proj_001', 'work', 'worker', '2026-01-01T01:00:00Z')"
        )
    monkeypatch.setattr(db, "_db_path", None)
    db.configure(path)

    with db.get_conn() as conn:
        row = conn.execute("SELECT started_at FROM projects WHERE id = 'proj_001'").fetchone()
    assert row["started_at"] == "2026-01-01T01:00:00Z"
