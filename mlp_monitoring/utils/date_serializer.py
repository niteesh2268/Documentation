from datetime import datetime
from typeguard import typechecked


@typechecked
def serialize_date(obj: datetime):
    """
    :param obj: datetime object
    :return: Date in ISO 8601 format
    """
    return obj.isoformat()
