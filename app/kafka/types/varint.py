import struct


def decode_varint(recv_message: bytes, signed: bool = False) -> tuple[int, int]:
    total = 0
    for i, value in enumerate(recv_message):
        total += (value & 0b01111111) << (i * 7)
        if value >> 7 == 0:
            return i + 1, total if not signed else total >> 1 if total % 2 == 0 else -(
                (total + 1) >> 1
            )
    return 0, 0


def encode_varint(total: int, signed: bool = False) -> bytes:
    total = total if not signed else total << 1 if total >= 0 else 2 * (-total) - 1
    data = b""
    while True:
        value = total & 0b01111111
        total >>= 7
        data += struct.pack("B", value | (1 << 7 if total > 0 else 0))
        if total == 0:
            break
    return data
