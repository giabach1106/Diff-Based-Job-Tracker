"""OpenAI-powered analysis engine for internship rows."""

from __future__ import annotations

import json
import logging
import time

from openai import APIConnectionError, APITimeoutError, InternalServerError, OpenAI, RateLimitError
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from config import Settings

LOGGER = logging.getLogger(__name__)


class JobAnalysis(BaseModel):
    """Structured output from LLM job analysis."""

    model_config = ConfigDict(extra="forbid", strict=True)

    company: str
    role: str
    location: str
    is_tech_intern: bool
    prestige_score: int = Field(ge=0, le=100)
    reason: str


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
            "company, role, location, is_tech_intern, prestige_score, reason. "
            "Classification: is_tech_intern=true only for SWE, Backend, Fullstack, AI/ML, DevOps, Quant. "
            "Set false for QA, Testing, PM, Marketing and other non-engineering tracks. "
            "Scoring rubric: 95+ for FAANG/HFT/unicorn-level (e.g., Stripe/OpenAI), "
            "85+ for strong tech firms/YC-scale startups, "
            "75+ for banks or major non-tech firms with engineering programs, "
            "below 70 for unknown or low relevance. "
            "Use concise reason text."
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
