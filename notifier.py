"""Notification handlers for Discord and optional Facebook."""

from __future__ import annotations

import logging
import time

import requests

from config import Settings
from llm_engine import JobAnalysis

LOGGER = logging.getLogger(__name__)


class Notifier:
    """Sends notifications to external channels."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.session = requests.Session()

    def send_discord(self, job: JobAnalysis, apply_link: str) -> None:
        """Send one Discord embed notification."""

        color = self._discord_color(job.prestige_score)
        location_fit = self._location_fit_label(job.location_priority.value)
        score_badge = self._score_badge(job.prestige_score)
        payload = {
            "embeds": [
                {
                    "title": f"{job.company} - {job.role}",
                    "url": apply_link,
                    "description": "High-quality tech internship match detected.",
                    "color": color,
                    "fields": [
                        {"name": "Company", "value": job.company[:1024], "inline": True},
                        {"name": "Role", "value": job.role[:1024], "inline": True},
                        {"name": "Location", "value": (job.location or "Unknown")[:1024], "inline": True},
                        {"name": "Location Fit", "value": location_fit, "inline": True},
                        {"name": "Score", "value": f"{job.prestige_score} ({score_badge})", "inline": True},
                        {"name": "Company Description", "value": job.company_description[:1024], "inline": False},
                        {"name": "Why This Match", "value": job.reason[:1024], "inline": False},
                        {"name": "Apply", "value": f"[Open application]({apply_link})", "inline": False},
                    ],
                }
            ]
        }

        retries = 3
        last_error: Exception | None = None
        for attempt in range(retries):
            try:
                response = self.session.post(
                    self.settings.discord_webhook_url,
                    json=payload,
                    timeout=self.settings.request_timeout_seconds,
                )
                if response.status_code in {429, 500, 502, 503, 504}:
                    raise requests.HTTPError(
                        f"Transient Discord error: {response.status_code}",
                        response=response,
                    )
                if response.status_code not in {200, 204}:
                    raise RuntimeError(
                        f"Discord webhook failed with status {response.status_code}: {response.text}"
                    )
                return
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError, RuntimeError) as exc:
                last_error = exc
                if attempt == retries - 1:
                    break
                sleep_seconds = 2**attempt
                LOGGER.warning("Discord send failed (%s). Retrying in %ss.", exc, sleep_seconds)
                time.sleep(sleep_seconds)

        assert last_error is not None
        raise last_error

    def send_facebook(self, job: JobAnalysis, apply_link: str) -> None:
        """Optional Facebook notifier stub (disabled by default)."""

        if not self.settings.enable_facebook:
            return

        if not self.settings.facebook_page_access_token or not self.settings.facebook_page_id:
            LOGGER.warning("Facebook notifications enabled but credentials are incomplete; skipping.")
            return

        LOGGER.info(
            "Facebook notifications are configured as a stub in this version. "
            "No Facebook API call is performed for %s.",
            job.company,
        )

    @staticmethod
    def _discord_color(score: int) -> int:
        if score > 85:
            return 0x2ECC71  # Green
        if score > 75:
            return 0xF1C40F  # Yellow
        return 0x95A5A6  # Neutral

    @staticmethod
    def _score_badge(score: int) -> str:
        if score >= 95:
            return "Elite"
        if score >= 85:
            return "Strong"
        if score >= 75:
            return "Good"
        return "Low"

    @staticmethod
    def _location_fit_label(location_priority: str) -> str:
        mapping = {
            "preferred": "Preferred (USA/Remote)",
            "neutral": "Neutral (Hybrid/Unknown)",
            "non_preferred": "Non-preferred (Non-USA onsite)",
        }
        return mapping.get(location_priority, "Unknown")
