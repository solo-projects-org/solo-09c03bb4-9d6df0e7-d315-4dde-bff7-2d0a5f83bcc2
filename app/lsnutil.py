"""PostgreSQL LSN（如 '1B3/4A0'）与整数的互转。"""


def lsn_to_int(s: str) -> int:
    hi, lo = s.split("/")
    return (int(hi, 16) << 32) + int(lo, 16)


def int_to_lsn(i: int) -> str:
    return f"{i >> 32:X}/{i & 0xFFFFFFFF:X}"
