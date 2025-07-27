import struct

from app.kafka.types.string import decode_string_compact


def decode_record_feature_level(
    buffer: bytes,
) -> tuple[int, dict[str, str | int]]:
    pos_name, name = decode_string_compact(buffer)
    buffer = buffer[pos_name:]
    total_length = pos_name

    (feature_level,) = struct.unpack(">H", buffer[:2])
    buffer = buffer[2:]
    total_length += 2

    return total_length, {
        "name": name,
        "feature_level": feature_level,
    }
