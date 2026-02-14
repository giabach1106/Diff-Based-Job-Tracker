"""GitHub API client for commit and diff retrieval."""

from __future__ import annotations

import base64
import difflib
import logging
import time
from typing import Any

import requests

from config import Settings

LOGGER = logging.getLogger(__name__)


class GitHubClient:
    """Client responsible for retrieving commits and diffs from GitHub."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = "https://api.github.com"
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.github+json",
                "User-Agent": "diff-based-job-tracker",
            }
        )
        if settings.github_token:
            self.session.headers["Authorization"] = f"Bearer {settings.github_token}"

    def get_latest_commit_sha(self) -> str:
        """Return latest commit SHA for the configured branch."""

        url = (
            f"{self.base_url}/repos/{self.settings.github_owner}/"
            f"{self.settings.github_repo}/branches/{self.settings.github_branch}"
        )
        payload = self._request_json(url)
        sha = payload.get("commit", {}).get("sha")
        if not sha:
            raise RuntimeError("Unable to resolve latest commit SHA from GitHub.")
        return str(sha)

    def get_commit_diff(self, old_sha: str, new_sha: str) -> list[str]:
        """Return added lines for the target file between two commits."""

        compare_url = (
            f"{self.base_url}/repos/{self.settings.github_owner}/"
            f"{self.settings.github_repo}/compare/{old_sha}...{new_sha}"
        )
        payload = self._request_json(compare_url)
        files = payload.get("files", [])

        for changed_file in files:
            if changed_file.get("filename") != self.settings.github_target_file:
                continue

            patch = changed_file.get("patch")
            if not patch:
                LOGGER.warning(
                    "Patch missing for %s in compare API. Falling back to file-level diff.",
                    self.settings.github_target_file,
                )
                return self._fallback_added_lines(old_sha, new_sha)

            return self._extract_added_lines_from_patch(str(patch))

        LOGGER.info("Target file %s not changed in compare result.", self.settings.github_target_file)
        return []

    def _extract_added_lines_from_patch(self, patch: str) -> list[str]:
        added_lines: list[str] = []
        for line in patch.splitlines():
            if line.startswith("+++"):
                continue
            if line.startswith("+"):
                added_lines.append(line[1:])
        return added_lines

    def _fallback_added_lines(self, old_sha: str, new_sha: str) -> list[str]:
        """Compute added lines by diffing full file contents at each SHA."""

        old_content = self._get_file_content_at_sha(old_sha)
        new_content = self._get_file_content_at_sha(new_sha)

        added_lines: list[str] = []
        diff_iter = difflib.unified_diff(
            old_content.splitlines(),
            new_content.splitlines(),
            lineterm="",
        )
        for line in diff_iter:
            if line.startswith("+++"):
                continue
            if line.startswith("+"):
                added_lines.append(line[1:])
        return added_lines

    def _get_file_content_at_sha(self, sha: str) -> str:
        """Download and decode a file from GitHub contents API at a commit SHA."""

        url = (
            f"{self.base_url}/repos/{self.settings.github_owner}/"
            f"{self.settings.github_repo}/contents/{self.settings.github_target_file}"
        )
        try:
            payload = self._request_json(url, params={"ref": sha})
        except requests.HTTPError as exc:
            response = exc.response
            if response is not None and response.status_code == 404:
                return ""
            raise

        if payload.get("encoding") != "base64":
            raise RuntimeError("Unexpected file encoding from GitHub contents API.")

        encoded = payload.get("content", "")
        return base64.b64decode(encoded).decode("utf-8", errors="replace")

    def _request_json(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        retries: int = 3,
    ) -> dict[str, Any]:
        """Perform a GET request with short exponential backoff retries."""

        last_error: Exception | None = None
        for attempt in range(retries):
            try:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.settings.request_timeout_seconds,
                )
                if response.status_code in {429, 500, 502, 503, 504}:
                    raise requests.HTTPError(
                        f"Transient GitHub error: {response.status_code}",
                        response=response,
                    )
                response.raise_for_status()
                return response.json()
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
                last_error = exc
                if attempt == retries - 1:
                    break
                sleep_seconds = 2**attempt
                LOGGER.warning("GitHub request failed (%s). Retrying in %ss.", exc, sleep_seconds)
                time.sleep(sleep_seconds)

        assert last_error is not None
        raise last_error
