from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class WebhookPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    event: str = Field(min_length=1, max_length=100)
    data: dict[str, Any]
