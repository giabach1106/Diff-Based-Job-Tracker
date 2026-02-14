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
        """Send Facebook notification via Messenger DM or Page feed."""

        if not self.settings.enable_facebook:
            return

        if not self.settings.facebook_page_access_token or not self.settings.facebook_page_id:
            LOGGER.warning("Facebook notifications enabled but credentials are incomplete; skipping.")
            return

        if self.settings.facebook_send_as_dm:
            if not self.settings.facebook_recipient_psid:
                LOGGER.warning("FACEBOOK_SEND_AS_DM=true but FACEBOOK_RECIPIENT_PSID is missing; skipping.")
                return
            self._send_facebook_dm(job, apply_link)
            return

        self._send_facebook_page_feed(job, apply_link)

    def _send_facebook_page_feed(self, job: JobAnalysis, apply_link: str) -> None:
        """Publish one post to Facebook Page feed."""

        endpoint = (
            f"https://graph.facebook.com/"
            f"{self.settings.facebook_graph_api_version}/"
            f"{self.settings.facebook_page_id}/feed"
        )
        payload = {
            "message": self._build_facebook_message(job, apply_link),
            "link": apply_link,
            "access_token": self.settings.facebook_page_access_token,
        }

        retries = 3
        last_error: Exception | None = None
        for attempt in range(retries):
            try:
                response = self.session.post(
                    endpoint,
                    data=payload,
                    timeout=self.settings.request_timeout_seconds,
                )
                if response.status_code in {429, 500, 502, 503, 504}:
                    raise requests.HTTPError(
                        f"Transient Facebook error: {response.status_code}",
                        response=response,
                    )

                response_json = {}
                try:
                    response_json = response.json()
                except ValueError:
                    response_json = {}

                if response.status_code >= 400:
                    error_payload = response_json.get("error", {})
                    if error_payload.get("is_transient"):
                        raise requests.HTTPError(
                            f"Transient Facebook Graph API error: {error_payload}",
                            response=response,
                        )
                    raise RuntimeError(
                        f"Facebook Graph API failed with status {response.status_code}: {response.text}"
                    )

                post_id = response_json.get("id")
                if post_id:
                    LOGGER.info("Facebook post published successfully: %s", post_id)
                else:
                    LOGGER.warning("Facebook post request succeeded but response had no post id.")
                return
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError, RuntimeError) as exc:
                last_error = exc
                if attempt == retries - 1:
                    break
                sleep_seconds = 2**attempt
                LOGGER.warning("Facebook send failed (%s). Retrying in %ss.", exc, sleep_seconds)
                time.sleep(sleep_seconds)

        assert last_error is not None
        raise last_error

    def _send_facebook_dm(self, job: JobAnalysis, apply_link: str) -> None:
        """Send one Messenger DM from the configured Page to a PSID."""

        endpoint = f"https://graph.facebook.com/{self.settings.facebook_graph_api_version}/me/messages"
        payload: dict[str, object] = {
            "recipient": {"id": self.settings.facebook_recipient_psid},
            "messaging_type": self.settings.facebook_messaging_type,
            "message": {"text": self._build_messenger_text(job, apply_link)},
        }

        if self.settings.facebook_messaging_type == "MESSAGE_TAG" and self.settings.facebook_message_tag:
            payload["tag"] = self.settings.facebook_message_tag

        retries = 3
        last_error: Exception | None = None
        for attempt in range(retries):
            try:
                response = self.session.post(
                    endpoint,
                    params={"access_token": self.settings.facebook_page_access_token},
                    json=payload,
                    timeout=self.settings.request_timeout_seconds,
                )

                if response.status_code in {429, 500, 502, 503, 504}:
                    raise requests.HTTPError(
                        f"Transient Messenger error: {response.status_code}",
                        response=response,
                    )

                response_json = {}
                try:
                    response_json = response.json()
                except ValueError:
                    response_json = {}

                if response.status_code >= 400:
                    error_payload = response_json.get("error", {})
                    if error_payload.get("is_transient"):
                        raise requests.HTTPError(
                            f"Transient Messenger Graph API error: {error_payload}",
                            response=response,
                        )
                    error_code = error_payload.get("code")
                    error_message = error_payload.get("message", response.text)
                    if error_code == 10:
                        raise RuntimeError(
                            "Messenger permission/window restriction. "
                            "Ensure user messaged the Page and app has required Messenger permissions. "
                            f"Graph error: {error_message}"
                        )
                    raise RuntimeError(
                        f"Messenger Graph API failed with status {response.status_code}: {response.text}"
                    )

                message_id = response_json.get("message_id")
                if message_id:
                    LOGGER.info("Facebook Messenger DM sent successfully: %s", message_id)
                else:
                    LOGGER.warning("Messenger send succeeded but response had no message_id.")
                return
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError, RuntimeError) as exc:
                last_error = exc
                if attempt == retries - 1:
                    break
                sleep_seconds = 2**attempt
                LOGGER.warning("Messenger send failed (%s). Retrying in %ss.", exc, sleep_seconds)
                time.sleep(sleep_seconds)

        assert last_error is not None
        raise last_error

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

    @staticmethod
    def _build_facebook_message(job: JobAnalysis, apply_link: str) -> str:
        lines = [
            "High-quality tech internship match",
            f"Company: {job.company}",
            f"Role: {job.role}",
            f"Location: {job.location or 'Unknown'}",
            f"Score: {job.prestige_score}",
            f"Company: {job.company_description}",
            f"Why: {job.reason}",
            f"Apply: {apply_link}",
        ]
        return "\n".join(lines)

    @staticmethod
    def _build_messenger_text(job: JobAnalysis, apply_link: str) -> str:
        lines = [
            "Internship Alert",
            f"{job.company} - {job.role}",
            f"Location: {job.location or 'Unknown'}",
            f"Score: {job.prestige_score}",
            f"Why: {job.reason}",
            f"Apply: {apply_link}",
        ]
        return "\n".join(lines)
