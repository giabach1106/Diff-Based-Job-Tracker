"""Main orchestration entrypoint for diff-based internship tracking."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import hashlib
import logging
import re
from datetime import date, datetime

from airtable_client import AirtableClient
from config import Settings, get_settings
from database import Database
from github_client import GitHubClient
from llm_engine import LLMEngine
from notifier import Notifier
from parsing_utils import (
    estimate_posted_date_from_age,
    extract_apply_link,
    extract_company_role_location,
    extract_posted_age,
    reconstruct_added_rows,
)

LOGGER = logging.getLogger(__name__)
URL_REGEX = re.compile(r"https?://[^\s<>\"]+")


@dataclass
class JobCandidate:
    row_payload: str
    apply_url: str
    company_fallback: str
    role_fallback: str
    posted_age: str | None = None
    posted_date: str | None = None


@dataclass
class GithubFetchResult:
    candidates: list[JobCandidate]
    current_sha: str | None
    should_update_sha: bool
    bootstrapped: bool = False


def _hash_link(link: str) -> str:
    return hashlib.sha256(link.encode("utf-8")).hexdigest()


def _stringify_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        for key in ("url", "href", "link", "name", "label", "title"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        parts = [part for part in (_stringify_value(v) for v in value.values()) if part]
        return ", ".join(parts)
    if isinstance(value, list):
        parts = [part for part in (_stringify_value(v) for v in value) if part]
        return ", ".join(parts)
    return str(value).strip()


def _extract_url_from_value(value: object) -> str | None:
    if value is None:
        return None

    if isinstance(value, str):
        text = value.strip()
        if text.startswith("http://") or text.startswith("https://"):
            return text
        match = URL_REGEX.search(text)
        if match:
            return match.group(0).rstrip(").,")
        return None

    if isinstance(value, dict):
        for key in ("url", "href", "link"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        for nested in value.values():
            found = _extract_url_from_value(nested)
            if found:
                return found
        return None

    if isinstance(value, list):
        for nested in value:
            found = _extract_url_from_value(nested)
            if found:
                return found
        return None

    return None


def _coerce_iso_date(value: object) -> str | None:
    text = _stringify_value(value)
    if not text:
        return None

    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        pass

    try:
        return datetime.fromisoformat(text).date().isoformat()
    except ValueError:
        return None


def _build_airtable_row_payload(fields: dict[str, object]) -> str:
    parts: list[str] = []
    for key, value in fields.items():
        value_text = _stringify_value(value)
        if not value_text:
            continue
        parts.append(f"{key}: {value_text}")
    return " | ".join(parts)


def _extract_apply_url_from_fields(fields: dict[str, object], preferred_field: str) -> str | None:
    primary = _extract_url_from_value(fields.get(preferred_field))
    if primary:
        return primary

    for key, value in fields.items():
        key_lower = key.lower()
        if "apply" not in key_lower and "job" not in key_lower and "link" not in key_lower and "url" not in key_lower:
            continue
        candidate = _extract_url_from_value(value)
        if candidate:
            return candidate
    return None


def _build_github_candidate(row: str) -> JobCandidate | None:
    apply_url = extract_apply_link(row)
    if not apply_url:
        return None

    posted_age = extract_posted_age(row)
    posted_date = estimate_posted_date_from_age(posted_age)
    company_fallback, role_fallback, _ = extract_company_role_location(row)
    return JobCandidate(
        row_payload=row,
        apply_url=apply_url,
        company_fallback=company_fallback or "Unknown",
        role_fallback=role_fallback or "Unknown",
        posted_age=posted_age,
        posted_date=posted_date,
    )


def _build_airtable_candidate(record: dict[str, object], settings: Settings) -> JobCandidate | None:
    fields_raw = record.get("fields", {})
    if not isinstance(fields_raw, dict):
        return None

    fields: dict[str, object] = dict(fields_raw)
    apply_url = _extract_apply_url_from_fields(fields, settings.airtable_apply_field)
    if not apply_url:
        return None

    company_fallback = _stringify_value(fields.get(settings.airtable_company_field)) or "Unknown"
    role_fallback = _stringify_value(fields.get(settings.airtable_role_field)) or "Unknown"
    posted_date = _coerce_iso_date(fields.get(settings.airtable_date_field))
    row_payload = _build_airtable_row_payload(fields)
    if not row_payload:
        row_payload = (
            f"Company: {company_fallback} | Role: {role_fallback} | "
            f"Location: {_stringify_value(fields.get(settings.airtable_location_field))}"
        )

    return JobCandidate(
        row_payload=row_payload,
        apply_url=apply_url,
        company_fallback=company_fallback,
        role_fallback=role_fallback,
        posted_age=None,
        posted_date=posted_date,
    )


def _process_candidate(
    *,
    db: Database,
    llm_engine: LLMEngine,
    notifier: Notifier,
    settings: Settings,
    row_payload: str,
    apply_url: str,
    company_fallback: str,
    role_fallback: str,
    posted_age: str | None = None,
    posted_date: str | None = None,
) -> None:
    link_hash = _hash_link(apply_url)
    if db.exists(link_hash):
        LOGGER.info("Skipping existing job: %s", apply_url)
        return

    notified = False
    try:
        analysis = llm_engine.analyze_job(row_payload)
    except Exception:
        LOGGER.exception("LLM analysis failed. Recording row as processed without notification.")
        db.insert_processed_job(
            link_hash=link_hash,
            company=company_fallback or "Unknown",
            role=role_fallback or "Unknown",
            score=0,
            notified=False,
        )
        return

    if analysis.is_tech_intern and analysis.prestige_score >= settings.min_notify_score:
        discord_sent = False
        facebook_sent = False
        try:
            notifier.send_discord(analysis, apply_url, posted_age=posted_age, posted_date=posted_date)
            discord_sent = True
        except Exception:
            LOGGER.exception("Failed to send Discord notification for %s", apply_url)

        if settings.enable_facebook:
            try:
                notifier.send_facebook(analysis, apply_url, posted_age=posted_age, posted_date=posted_date)
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


def _process_candidates(
    *,
    candidates: list[JobCandidate],
    db: Database,
    llm_engine: LLMEngine,
    notifier: Notifier,
    settings: Settings,
) -> None:
    for candidate in candidates:
        _process_candidate(
            db=db,
            llm_engine=llm_engine,
            notifier=notifier,
            settings=settings,
            row_payload=candidate.row_payload,
            apply_url=candidate.apply_url,
            company_fallback=candidate.company_fallback,
            role_fallback=candidate.role_fallback,
            posted_age=candidate.posted_age,
            posted_date=candidate.posted_date,
        )


def _resolve_airtable_identifiers(settings: Settings) -> None:
    source_hint = settings.airtable_shared_view_url or ""
    if (not settings.airtable_base_id or not settings.airtable_table_id) and source_hint:
        parsed_base_id, parsed_table_id = AirtableClient.parse_identifiers_from_shared_url(source_hint)
        if parsed_base_id and not settings.airtable_base_id:
            settings.airtable_base_id = parsed_base_id
        if parsed_table_id and not settings.airtable_table_id:
            settings.airtable_table_id = parsed_table_id


def _fetch_github_candidates(settings: Settings, last_sha: str | None) -> GithubFetchResult:
    github_client = GitHubClient(settings)
    current_sha = github_client.get_latest_commit_sha()

    if not last_sha:
        return GithubFetchResult(
            candidates=[],
            current_sha=current_sha,
            should_update_sha=True,
            bootstrapped=True,
        )

    if last_sha == current_sha:
        return GithubFetchResult(
            candidates=[],
            current_sha=current_sha,
            should_update_sha=False,
            bootstrapped=False,
        )

    added_lines = github_client.get_commit_diff(last_sha, current_sha)
    rows = reconstruct_added_rows(added_lines)
    candidates = [candidate for row in rows if (candidate := _build_github_candidate(row))]
    LOGGER.info("Detected %d GitHub candidate row(s) to process.", len(candidates))
    return GithubFetchResult(
        candidates=candidates,
        current_sha=current_sha,
        should_update_sha=True,
        bootstrapped=False,
    )


def _fetch_airtable_candidates(settings: Settings) -> list[JobCandidate]:
    _resolve_airtable_identifiers(settings)
    if not settings.airtable_pat:
        raise ValueError("AIRTABLE_PAT is required when Airtable source is enabled.")
    if not settings.airtable_base_id or not settings.airtable_table_id:
        raise ValueError(
            "Airtable source requires AIRTABLE_BASE_ID and AIRTABLE_TABLE_ID, "
            "or AIRTABLE_SHARED_VIEW_URL that contains both ids."
        )

    airtable_client = AirtableClient(settings)
    records = airtable_client.list_records()
    LOGGER.info("Fetched %d Airtable record(s).", len(records))
    candidates: list[JobCandidate] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        candidate = _build_airtable_candidate(record, settings)
        if candidate:
            candidates.append(candidate)
    LOGGER.info("Detected %d Airtable candidate row(s) to process.", len(candidates))
    return candidates


def _run_from_github(*, settings: Settings, db: Database, llm_engine: LLMEngine, notifier: Notifier) -> int:
    result = _fetch_github_candidates(settings, db.get_last_commit_sha())

    if result.bootstrapped and result.current_sha:
        LOGGER.info("No previous commit found. Bootstrapping with current SHA: %s", result.current_sha)
        db.set_last_commit_sha(result.current_sha)
        return 0

    if not result.should_update_sha:
        LOGGER.info("No new commits since last run. Exiting.")
        return 0

    _process_candidates(
        candidates=result.candidates,
        db=db,
        llm_engine=llm_engine,
        notifier=notifier,
        settings=settings,
    )
    if result.current_sha:
        db.set_last_commit_sha(result.current_sha)
        LOGGER.info("Run completed. Updated last processed SHA to %s", result.current_sha)
    return 0


def _run_from_airtable(*, settings: Settings, db: Database, llm_engine: LLMEngine, notifier: Notifier) -> int:
    candidates = _fetch_airtable_candidates(settings)
    _process_candidates(
        candidates=candidates,
        db=db,
        llm_engine=llm_engine,
        notifier=notifier,
        settings=settings,
    )
    LOGGER.info("Airtable run completed.")
    return 0


def _run_from_both(*, settings: Settings, db: Database, llm_engine: LLMEngine, notifier: Notifier) -> int:
    last_sha = db.get_last_commit_sha()

    github_result: GithubFetchResult | None = None
    airtable_candidates: list[JobCandidate] = []
    github_error: Exception | None = None
    airtable_error: Exception | None = None

    with ThreadPoolExecutor(max_workers=2) as executor:
        github_future = executor.submit(_fetch_github_candidates, settings, last_sha)
        airtable_future = executor.submit(_fetch_airtable_candidates, settings)

        try:
            github_result = github_future.result()
        except Exception as exc:  # pragma: no cover - external API failures
            github_error = exc
            LOGGER.exception("GitHub source failed in SOURCE_TYPE=both.")

        try:
            airtable_candidates = airtable_future.result()
        except Exception as exc:  # pragma: no cover - external API failures
            airtable_error = exc
            LOGGER.exception("Airtable source failed in SOURCE_TYPE=both.")

    if github_result:
        if github_result.bootstrapped and github_result.current_sha:
            LOGGER.info("No previous commit found. Bootstrapping with current SHA: %s", github_result.current_sha)
        elif not github_result.should_update_sha:
            LOGGER.info("No new commits since last run for GitHub source.")

        _process_candidates(
            candidates=github_result.candidates,
            db=db,
            llm_engine=llm_engine,
            notifier=notifier,
            settings=settings,
        )
        if github_result.should_update_sha and github_result.current_sha:
            db.set_last_commit_sha(github_result.current_sha)
            LOGGER.info("Updated last processed GitHub SHA to %s", github_result.current_sha)

    _process_candidates(
        candidates=airtable_candidates,
        db=db,
        llm_engine=llm_engine,
        notifier=notifier,
        settings=settings,
    )

    if github_error and airtable_error:
        raise RuntimeError("Both GitHub and Airtable sources failed.")

    if github_error or airtable_error:
        LOGGER.warning("One source failed, but the other source was processed successfully.")

    LOGGER.info("Combined run completed.")
    return 0


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
        llm_engine = LLMEngine(settings)

        if settings.facebook_auto_use_captured_psid and not settings.facebook_recipient_psid:
            captured_psid = db.get_state("facebook_recipient_psid")
            if captured_psid:
                settings.facebook_recipient_psid = captured_psid
                LOGGER.info("Loaded Facebook PSID from DB state.")

        notifier = Notifier(settings)
        source_type = settings.source_type.strip().lower()
        if source_type == "github":
            return _run_from_github(settings=settings, db=db, llm_engine=llm_engine, notifier=notifier)
        if source_type == "airtable":
            return _run_from_airtable(settings=settings, db=db, llm_engine=llm_engine, notifier=notifier)
        if source_type == "both":
            return _run_from_both(settings=settings, db=db, llm_engine=llm_engine, notifier=notifier)

        raise ValueError("SOURCE_TYPE must be one of: 'github', 'airtable', 'both'.")
    except Exception:
        LOGGER.exception("Fatal error during run.")
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(run_once())
