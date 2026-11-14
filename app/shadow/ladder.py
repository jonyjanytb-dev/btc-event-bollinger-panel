"""Validated three-step stake schedule, independent of trade direction."""
from decimal import Decimal, InvalidOperation


class LadderError(ValueError):
    pass


def validate_stakes(values, maximum: str) -> tuple[str, str, str]:
    if not isinstance(values, list) or len(values) != 3:
        raise LadderError("INVALID_LADDER_STAKES")
    try:
        cap = Decimal(maximum)
        stakes = [Decimal(value) for value in values]
    except (InvalidOperation, TypeError):
        raise LadderError("INVALID_LADDER_STAKES") from None
    if (not all(type(value) is str for value in values)
            or not cap.is_finite() or cap < 5
            or any(not stake.is_finite() or stake < 5 or stake > cap
                   or stake.as_tuple().exponent < -2 for stake in stakes)):
        raise LadderError("INVALID_LADDER_STAKES")
    return tuple(str(stake) for stake in stakes)


def next_step(step: int, result: str) -> int:
    if type(step) is not int or step not in (0, 1, 2):
        raise LadderError("INVALID_LADDER_STEP")
    if result in ("WIN", "EQUAL"):
        return 0
    if result == "LOSS":
        return (step + 1) % 3
    raise LadderError("RESULT_UNVERIFIED")
