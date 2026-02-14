"""Utilities for parsing added diff lines into job row data."""

from __future__ import annotations

import html
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

HREF_REGEX = re.compile(r"href\s*=\s*[\"']([^\"']+)[\"']", re.IGNORECASE)
APPLY_ANCHOR_REGEX = re.compile(
    r"<a[^>]+href\s*=\s*[\"']([^\"']+)[\"'][^>]*>\s*(?:<img[^>]*alt\s*=\s*[\"']Apply[\"']|Apply\b)",
    re.IGNORECASE,
)
TAG_REGEX = re.compile(r"<[^>]+>")
MARKDOWN_URL_REGEX = re.compile(r"\[(https?://[^\]]+)\]\((https?://[^)]+)\)", re.IGNORECASE)
AGE_TOKEN_REGEX = re.compile(r"^\s*(\d+)\s*(h|d|w|mo)\s*$", re.IGNORECASE)

TRACKING_QUERY_KEYS = {"fbclid", "gclid", "igshid", "ref", "source"}
IMAGE_DOMAINS = {
    "imgur.com",
    "i.imgur.com",
    "raw.githubusercontent.com",
    "user-images.githubusercontent.com",
}
IMAGE_EXTENSIONS = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico")


def reconstruct_added_rows(added_lines: list[str]) -> list[str]:
    """Reconstruct complete added table rows from added diff lines."""

    rows: list[str] = []
    buffer: list[str] = []
    collecting = False

    for raw_line in added_lines:
        line = raw_line.strip()
        lower = line.lower()

        if "<tr" in lower:
            collecting = True
            buffer = [line]
            if "</tr>" in lower:
                rows.append(" ".join(buffer))
                buffer = []
                collecting = False
            continue

        if collecting:
            buffer.append(line)
            if "</tr>" in lower:
                rows.append(" ".join(buffer))
                buffer = []
                collecting = False

    if rows:
        return rows

    # Fallback: aggregate contiguous html chunks that look like row content.
    chunks: list[str] = []
    current: list[str] = []
    for raw_line in added_lines:
        line = raw_line.strip()
        if not line:
            if current:
                chunk = " ".join(current)
                if "<td" in chunk.lower():
                    chunks.append(chunk)
                current = []
            continue
        current.append(line)

    if current:
        chunk = " ".join(current)
        if "<td" in chunk.lower():
            chunks.append(chunk)

    return chunks


def extract_apply_link(raw_html_string: str) -> str | None:
    """Extract and normalize the best apply link candidate from an HTML row."""

    for match in APPLY_ANCHOR_REGEX.findall(raw_html_string):
        candidate = _normalize_candidate_url(match)
        if candidate and _is_valid_apply_link(candidate):
            return candidate

    for match in HREF_REGEX.findall(raw_html_string):
        candidate = _normalize_candidate_url(match)
        if candidate and _is_valid_apply_link(candidate):
            return candidate

    return None


def extract_company_role_location(row_html: str) -> tuple[str | None, str | None, str | None]:
    """Best-effort extraction of company, role, and location from a row."""

    cells = re.findall(r"<td[^>]*>(.*?)</td>", row_html, flags=re.IGNORECASE | re.DOTALL)
    if not cells:
        return None, None, None

    company = _clean_text(cells[0]) if len(cells) >= 1 else None
    role = _clean_text(cells[1]) if len(cells) >= 2 else None
    location = _clean_text(cells[2]) if len(cells) >= 3 else None

    return company or None, role or None, location or None


def extract_posted_age(row_html: str) -> str | None:
    """Extract relative age token from a table row (e.g. 0d, 3d, 1w, 2mo)."""

    cells = re.findall(r"<td[^>]*>(.*?)</td>", row_html, flags=re.IGNORECASE | re.DOTALL)
    if len(cells) < 5:
        return None

    age_text = _clean_text(cells[4])
    if not age_text:
        return None
    if not AGE_TOKEN_REGEX.match(age_text):
        return None
    return age_text.lower()


def estimate_posted_date_from_age(age_token: str | None) -> str | None:
    """Convert an age token into an approximate UTC posting date (YYYY-MM-DD)."""

    if not age_token:
        return None

    match = AGE_TOKEN_REGEX.match(age_token)
    if not match:
        return None

    amount = int(match.group(1))
    unit = match.group(2).lower()
    now = datetime.now(timezone.utc)

    if unit == "h":
        posted = now - timedelta(hours=amount)
    elif unit == "d":
        posted = now - timedelta(days=amount)
    elif unit == "w":
        posted = now - timedelta(weeks=amount)
    elif unit == "mo":
        posted = now - timedelta(days=amount * 30)
    else:
        return None

    return posted.date().isoformat()


def _normalize_candidate_url(raw_url: str) -> str | None:
    text = html.unescape(raw_url.strip())
    if not text:
        return None

    markdown_match = MARKDOWN_URL_REGEX.search(text)
    if markdown_match:
        text = markdown_match.group(2)

    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1].strip()

    parsed = urlsplit(text)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return None

    query_items = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        key_lower = key.lower()
        if key_lower.startswith("utm_"):
            continue
        if key_lower in TRACKING_QUERY_KEYS:
            continue
        query_items.append((key, value))

    normalized_path = parsed.path.rstrip("/") or parsed.path
    normalized_query = urlencode(query_items, doseq=True)

    normalized = urlunsplit(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            normalized_path,
            normalized_query,
            "",
        )
    )
    return normalized


def _is_valid_apply_link(url: str) -> bool:
    parsed = urlsplit(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()

    if any(image_domain in host for image_domain in IMAGE_DOMAINS):
        return False
    if path.endswith(IMAGE_EXTENSIONS):
        return False
    if "logo" in path or "icon" in path:
        return False

    # Common non-apply company profile links in Simplify table rows.
    if host.endswith("simplify.jobs") and path.startswith("/c/"):
        return False

    # Exclude obvious repository/documentation links.
    if "github.com" in host and "jobs" not in path:
        return False

    return True


def _clean_text(raw: str) -> str:
    text = TAG_REGEX.sub(" ", raw)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()
