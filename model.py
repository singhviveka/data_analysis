from pydantic import BaseModel


class TimePeriod(BaseModel):
    start_date: str
    end_date: str
