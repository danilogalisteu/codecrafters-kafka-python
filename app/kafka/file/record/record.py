import logging

from app.kafka.types.varint import decode_varint

from .value import decode_record_value


def decode_record(
    buffer: bytes,
) -> tuple[int, dict[str, str | int | bytes | list[int] | list[bytes] | list[str]]]:
    logging.debug("pre record %s", buffer[:40].hex(" "))

    pos_length, length = decode_varint(buffer, signed=True)
    if pos_length == 0:
        return 0, {}
    buffer = buffer[pos_length:]
    total_length = pos_length

    record_info = {"record_length": length}

    if len(buffer) < 1:
        return 0, record_info
    record_info["attr"] = buffer[0]
    buffer = buffer[1:]
    total_length += 1

    pos_ts, record_info["ts_delta"] = decode_varint(buffer, signed=True)
    if pos_ts == 0:
        return 0, record_info
    buffer = buffer[pos_ts:]
    total_length += pos_ts

    pos_offset, record_info["offset_delta"] = decode_varint(buffer, signed=True)
    if pos_offset == 0:
        return 0, record_info
    buffer = buffer[pos_offset:]
    total_length += pos_offset

    pos_key_length, key_length = decode_varint(buffer, signed=True)
    if pos_key_length == 0:
        return 0, record_info
    buffer = buffer[pos_key_length:]
    total_length += pos_key_length

    if len(buffer) < key_length - 1:
        return 0, record_info
    record_info["key"] = b""
    if key_length > -1:
        record_info["key"] = buffer[:key_length]
        buffer = buffer[key_length:]

    pos_value, record_data = decode_record_value(buffer)
    if pos_value == 0:
        return 0, record_info
    buffer = buffer[pos_value:]
    total_length += pos_value
    record_info = {**record_info, **record_data}

    pos_headers, headers_count = decode_varint(buffer)
    if pos_headers == 0:
        return 0, record_info
    buffer = buffer[pos_headers:]
    total_length += pos_headers

    headers: list[str] = []
    for _ in range(headers_count):
        logging.error(
            "unhandled header (expected count %d)\nbuffer %s",
            headers_count,
            buffer[:20].hex(" "),
        )
        record_info["headers"] = headers
        return (
            pos_length + length,
            record_info,
        )

    record_info["headers"] = headers
    return (total_length, record_info)
