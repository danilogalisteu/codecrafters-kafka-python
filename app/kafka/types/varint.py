def decode_varint(recv_message):
    total = 0
    for i, value in enumerate(recv_message):
        total += (value & 0b01111111) << (i * 7)
        if value >> 7 == 0:
            return i + 1, total
    return 0, 0
