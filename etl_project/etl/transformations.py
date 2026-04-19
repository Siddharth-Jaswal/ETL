from __future__ import annotations

from typing import Any


def transform_record(record: dict[str, Any]) -> tuple[str, str, str, str]:
    transformed_name = str(record["name"]).upper()
    transformed_roll_no = f"RN_{record['roll_no']}"
    transformed_email = str(record["email"]).upper()
    transformed_phone_number = _prefix_country_code(str(record["phone_number"]))
    return (
        transformed_name,
        transformed_roll_no,
        transformed_email,
        transformed_phone_number,
    )


def _prefix_country_code(phone_number: str) -> str:
    return phone_number if phone_number.startswith("+91") else f"+91{phone_number}"
