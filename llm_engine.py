"""OpenAI-powered analysis engine for internship rows."""

from __future__ import annotations

import json
import logging
import time
from enum import Enum
from typing import Any

from openai import APIConnectionError, APITimeoutError, InternalServerError, OpenAI, RateLimitError
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from config import Settings

LOGGER = logging.getLogger(__name__)


class LocationPriority(str, Enum):
    """Location preference level for notification quality."""

    PREFERRED = "preferred"
    NEUTRAL = "neutral"
    NON_PREFERRED = "non_preferred"


class JobAnalysis(BaseModel):
    """Structured output from LLM job analysis."""

    model_config = ConfigDict(extra="forbid", strict=True)

    company: str
    role: str
    location: str
    company_description: str = "No company description provided."
    is_tech_intern: bool
    prestige_score: int = Field(ge=0, le=100)
    location_priority: LocationPriority = LocationPriority.NEUTRAL
    reason: str

    @field_validator("location_priority", mode="before")
    @classmethod
    def _normalize_location_priority(cls, value: Any) -> LocationPriority:
        if isinstance(value, LocationPriority):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            try:
                return LocationPriority(normalized)
            except ValueError as exc:
                raise ValueError(
                    "location_priority must be one of: preferred, neutral, non_preferred"
                ) from exc
        raise TypeError("location_priority must be a LocationPriority enum or valid string")


class LLMEngine:
    """Wrapper around OpenAI for row-level job analysis."""

    SYSTEM_PROMPT = "You are an elite Tech Recruiter."

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = OpenAI(api_key=settings.openai_api_key)

    def analyze_job(self, raw_html: str) -> JobAnalysis:
        """Analyze one table row and return validated structured output."""

        user_prompt = (
            "Analyze this internship listing row and return strict JSON with fields: "
            "company, role, location, company_description, is_tech_intern, prestige_score, "
            "location_priority, reason. "
            "company_description must be one sentence describing the company's industry and product focus. "
            "Classification: is_tech_intern=true only for SWE, Backend, Fullstack, AI/ML, DevOps, Quant, SDE, Software Engineer, Software Development. "
            "Set false for QA, Testing, PM, Marketing and other non-engineering tracks. "
            "Scoring rubric baseline: 95+ for FAANG/HFT/unicorn-level (e.g., Stripe/OpenAI), "
            "85+ for strong tech firms/YC-scale startups, "
            "75+ for banks or major non-tech firms with engineering programs, "
            "below 70 for unknown or low relevance. "
            "Location priority rule: set location_priority='preferred' for USA or fully Remote roles; "
            "set location_priority='neutral' for hybrid or unclear location; "
            "set location_priority='non_preferred' for clearly non-USA onsite roles. "
            "Apply a location weighting to prestige_score after baseline: preferred +8, neutral +0, non_preferred -8, "
            "and clamp between 0 and 100. "
            "reason must be concise and mention both company quality and location priority impact."
            f"\n\nRaw row:\n{raw_html}"
        )

        retries = 2
        last_error: Exception | None = None

        for attempt in range(retries):
            try:
                response = self.client.chat.completions.create(
                    model=self.settings.openai_model,
                    temperature=0.1,
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": self.SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt},
                    ],
                )
                content = response.choices[0].message.content
                if not content:
                    raise ValueError("OpenAI returned empty content.")

                payload = json.loads(content)
                return JobAnalysis.model_validate(payload)
            except (APIConnectionError, APITimeoutError, RateLimitError, InternalServerError) as exc:
                last_error = exc
                if attempt == retries - 1:
                    break
                sleep_seconds = 2**attempt
                LOGGER.warning("Transient OpenAI error (%s). Retrying in %ss.", exc, sleep_seconds)
                time.sleep(sleep_seconds)
            except (json.JSONDecodeError, ValidationError) as exc:
                raise ValueError(f"Invalid structured output from LLM: {exc}") from exc

        assert last_error is not None
        raise last_error
