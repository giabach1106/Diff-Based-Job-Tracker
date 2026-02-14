"""Verbose debug runner for diff processing and notification decisions."""

from __future__ import annotations

import argparse
import hashlib
import logging
from dataclasses import dataclass

from config import get_settings
from database import Database
from github_client import GitHubClient
from llm_engine import LLMEngine
from notifier import Notifier
from parsing_utils import extract_apply_link, extract_company_role_location, reconstruct_added_rows

LOGGER = logging.getLogger(__name__)


@dataclass
class Counters:
    total_rows: int = 0
    no_apply_link: int = 0
    already_processed: int = 0
    llm_failed: int = 0
    skipped_not_tech: int = 0
    skipped_low_score: int = 0
    eligible: int = 0
    discord_ok: int = 0
    discord_failed: int = 0
    facebook_ok: int = 0
    facebook_failed: int = 0


def _hash_link(link: str) -> str:
    return hashlib.sha256(link.encode("utf-8")).hexdigest()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Debug diff processing and notification filters.")
    parser.add_argument("--max-rows", type=int, default=0, help="Process at most N reconstructed rows (0 = all).")
    parser.add_argument("--send", action="store_true", help="Actually send Discord/Facebook notifications.")
    parser.add_argument(
        "--include-processed",
        action="store_true",
        help="Include links already present in processed_jobs (default skips them).",
    )
    parser.add_argument("--old-sha", default="", help="Override old/base SHA. Defaults to DB last_commit_sha.")
    parser.add_argument("--new-sha", default="", help="Override new/head SHA. Defaults to latest branch SHA.")
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    args = _parse_args()

    settings = get_settings()
    db = Database(settings.database_path)
    db.init_schema()

    try:
        if settings.facebook_auto_use_captured_psid and not settings.facebook_recipient_psid:
            captured_psid = db.get_state("facebook_recipient_psid")
            if captured_psid:
                settings.facebook_recipient_psid = captured_psid

        gh = GitHubClient(settings)
        llm = LLMEngine(settings)
        notifier = Notifier(settings)

        old_sha = args.old_sha.strip() or (db.get_last_commit_sha() or "")
        new_sha = args.new_sha.strip() or gh.get_latest_commit_sha()

        if not old_sha:
            print("No old SHA (DB last_commit_sha is empty). Use --old-sha or bootstrap first.")
            return 1

        print(f"old_sha={old_sha}")
        print(f"new_sha={new_sha}")
        print(f"min_notify_score={settings.min_notify_score}")
        print(f"enable_facebook={settings.enable_facebook}, facebook_send_as_dm={settings.facebook_send_as_dm}")
        print(f"send_mode={'ON' if args.send else 'OFF (dry-run)'}")
        print()

        added_lines = gh.get_commit_diff(old_sha, new_sha)
        rows = reconstruct_added_rows(added_lines)
        if args.max_rows > 0:
            rows = rows[: args.max_rows]

        counters = Counters(total_rows=len(rows))
        print(f"reconstructed_rows={len(rows)}")

        for idx, row in enumerate(rows, 1):
            apply_link = extract_apply_link(row)
            if not apply_link:
                counters.no_apply_link += 1
                print(f"[{idx}] SKIP no_apply_link")
                continue

            link_hash = _hash_link(apply_link)
            if db.exists(link_hash) and not args.include_processed:
                counters.already_processed += 1
                print(f"[{idx}] SKIP already_processed link={apply_link}")
                continue

            fallback_company, fallback_role, fallback_location = extract_company_role_location(row)

            try:
                analysis = llm.analyze_job(row)
            except Exception as exc:  # pragma: no cover - network/model errors
                counters.llm_failed += 1
                print(
                    f"[{idx}] SKIP llm_failed company={fallback_company or 'Unknown'} "
                    f"role={fallback_role or 'Unknown'} error={exc}"
                )
                continue

            reasons: list[str] = []
            if not analysis.is_tech_intern:
                counters.skipped_not_tech += 1
                reasons.append("not_tech")
            if analysis.prestige_score < settings.min_notify_score:
                counters.skipped_low_score += 1
                reasons.append(f"score<{settings.min_notify_score}")

            company = analysis.company or fallback_company or "Unknown"
            role = analysis.role or fallback_role or "Unknown"
            location = analysis.location or fallback_location or "Unknown"

            if reasons:
                print(
                    f"[{idx}] SKIP {','.join(reasons)} company={company} role={role} "
                    f"score={analysis.prestige_score} tech={analysis.is_tech_intern} "
                    f"reputation={analysis.company_reputation.value} location={location}"
                )
                continue

            counters.eligible += 1
            print(
                f"[{idx}] ELIGIBLE company={company} role={role} score={analysis.prestige_score} "
                f"tech={analysis.is_tech_intern} reputation={analysis.company_reputation.value} "
                f"location={location}"
            )
            print(f"      apply={apply_link}")

            if not args.send:
                continue

            try:
                notifier.send_discord(analysis, apply_link)
                counters.discord_ok += 1
                print(f"      discord=ok")
            except Exception as exc:  # pragma: no cover - external API failures
                counters.discord_failed += 1
                print(f"      discord=failed error={exc}")

            if settings.enable_facebook:
                try:
                    notifier.send_facebook(analysis, apply_link)
                    counters.facebook_ok += 1
                    print(f"      facebook=ok")
                except Exception as exc:  # pragma: no cover - external API failures
                    counters.facebook_failed += 1
                    print(f"      facebook=failed error={exc}")

        print("\nSummary")
        print(f"total_rows={counters.total_rows}")
        print(f"no_apply_link={counters.no_apply_link}")
        print(f"already_processed={counters.already_processed}")
        print(f"llm_failed={counters.llm_failed}")
        print(f"skipped_not_tech={counters.skipped_not_tech}")
        print(f"skipped_low_score={counters.skipped_low_score}")
        print(f"eligible={counters.eligible}")
        print(f"discord_ok={counters.discord_ok}")
        print(f"discord_failed={counters.discord_failed}")
        print(f"facebook_ok={counters.facebook_ok}")
        print(f"facebook_failed={counters.facebook_failed}")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
