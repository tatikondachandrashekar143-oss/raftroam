
from typing import Literal

from pydantic import BaseModel, Field


class TravelRequest(BaseModel):
    origin: str
    destination: str
    arrival_before: str
    budget: float = Field(gt=0)
    passengers: int = Field(default=1, ge=1)
    bus_preference: Literal["sleeper", "seater", "any"] = "any"


class Itinerary(BaseModel):
    bus_id: str
    fare: float
    arrival_time: str
    transfer_risk: Literal["low", "medium", "high"]
    simulated: bool = True


class TravelPlanResponse(BaseModel):
    options: list[Itinerary]
    message: str = "Simulated travel options"
