from pydantic import BaseModel, Field
from typing import Dict, Optional

class WildfireRequest(BaseModel):
    features: Dict[str, float]

class WildfireResponse(BaseModel):
    wildfire_probability: float
    wildfire_prediction: int
    model_name: Optional[str] = None
    confidence: Optional[float] = None
