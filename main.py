"""Main orchestration entrypoint for diff-based internship tracking."""

from __future__ import annotations

import hashlib
import logging

from config import get_settings
from database import Database
from github_client import GitHubClient
from llm_engine import LLMEngine
from notifier import Notifier
from parsing_utils import extract_apply_link, extract_company_role_location, reconstruct_added_rows

LOGGER = logging.getLogger(__name__)


def _hash_link(link: str) -> str:
    return hashlib.sha256(link.encode("utf-8")).hexdigest()


def run_once() -> int:
    """Execute one polling cycle."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    settings = get_settings()
    db = Database(settings.database_path)

    try:
        db.init_schema()
        github_client = GitHubClient(settings)
        llm_engine = LLMEngine(settings)

        if settings.facebook_auto_use_captured_psid and not settings.facebook_recipient_psid:
            captured_psid = db.get_state("facebook_recipient_psid")
            if captured_psid:
                settings.facebook_recipient_psid = captured_psid
                LOGGER.info("Loaded Facebook PSID from DB state.")

        notifier = Notifier(settings)

        last_sha = db.get_last_commit_sha()
        current_sha = github_client.get_latest_commit_sha()

        if not last_sha:
            LOGGER.info("No previous commit found. Bootstrapping with current SHA: %s", current_sha)
            db.set_last_commit_sha(current_sha)
            return 0

        if last_sha == current_sha:
            LOGGER.info("No new commits since last run. Exiting.")
            return 0

        added_lines = github_client.get_commit_diff(last_sha, current_sha)
        rows = reconstruct_added_rows(added_lines)
        LOGGER.info("Detected %d candidate added row(s) to process.", len(rows))

        for row in rows:
            apply_url = extract_apply_link(row)
            if not apply_url:
                continue

            link_hash = _hash_link(apply_url)
            if db.exists(link_hash):
                LOGGER.info("Skipping existing job (Age update): %s", apply_url)
                continue

            notified = False
            company_fallback, role_fallback, _ = extract_company_role_location(row)

            try:
                analysis = llm_engine.analyze_job(row)
            except Exception:
                LOGGER.exception("LLM analysis failed. Recording row as processed without notification.")
                db.insert_processed_job(
                    link_hash=link_hash,
                    company=company_fallback or "Unknown",
                    role=role_fallback or "Unknown",
                    score=0,
                    notified=False,
                )
                continue

            if analysis.is_tech_intern and analysis.prestige_score >= settings.min_notify_score:
                discord_sent = False
                facebook_sent = False
                try:
                    notifier.send_discord(analysis, apply_url)
                    discord_sent = True
                except Exception:
                    LOGGER.exception("Failed to send Discord notification for %s", apply_url)

                if settings.enable_facebook:
                    try:
                        notifier.send_facebook(analysis, apply_url)
                        facebook_sent = True
                    except Exception:
                        LOGGER.exception("Failed to send Facebook notification for %s", apply_url)

                notified = discord_sent or facebook_sent
            else:
                LOGGER.info(
                    "Skipping notification company=%s role=%s score=%s tech=%s reputation=%s",
                    analysis.company,
                    analysis.role,
                    analysis.prestige_score,
                    analysis.is_tech_intern,
                    analysis.company_reputation.value,
                )

            db.insert_processed_job(
                link_hash=link_hash,
                company=analysis.company,
                role=analysis.role,
                score=analysis.prestige_score,
                notified=notified,
            )

        db.set_last_commit_sha(current_sha)
        LOGGER.info("Run completed. Updated last processed SHA to %s", current_sha)
        return 0
    except Exception:
        LOGGER.exception("Fatal error during run.")
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(run_once())
