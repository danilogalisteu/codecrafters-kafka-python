import logging

from app.kafka.types.varint import decode_varint

from .value import decode_record_value


def decode_record(
    buffer: bytes,
) -> tuple[
    int,
    int,
    int,
    int,
    int,
    bytes,
    int,
    int,
    int,
    dict[str, str | int | bytes | list[str]],
    list[str],
    list[str],
]:
    logging.debug("pre record %s", buffer[:40].hex(" "))

    pos_length, length = decode_varint(buffer, signed=True)
    if pos_length == 0:
        return 0, 0, 0, 0, 0, b"", 0, 0, 0, {}, [], []
    buffer = buffer[pos_length:]
    total_length = pos_length

    if len(buffer) < 1:
        return 0, 0, 0, 0, 0, b"", 0, 0, 0, {}, [], []
    attr = buffer[0]
    buffer = buffer[1:]
    total_length += 1

    pos_ts, ts_delta = decode_varint(buffer, signed=True)
    if pos_ts == 0:
        return 0, 0, 0, 0, 0, b"", 0, 0, 0, {}, [], []
    buffer = buffer[pos_ts:]
    total_length += pos_ts

    pos_offset, offset_delta = decode_varint(buffer, signed=True)
    if pos_offset == 0:
        return 0, 0, 0, 0, 0, b"", 0, 0, 0, {}, [], []
    buffer = buffer[pos_offset:]
    total_length += pos_offset

    pos_key_length, key_length = decode_varint(buffer, signed=True)
    if pos_key_length == 0:
        return 0, 0, 0, 0, 0, b"", 0, 0, 0, {}, [], []
    buffer = buffer[pos_key_length:]
    total_length += pos_key_length

    if len(buffer) < key_length - 1:
        return 0, 0, 0, 0, 0, b"", 0, 0, 0, {}, [], []
    key = b""
    if key_length > -1:
        key = buffer[:key_length]
        buffer = buffer[key_length:]

    pos_value, frame_version, record_type, record_version, record_data, fields = (
        decode_record_value(buffer)
    )
    if pos_value == 0:
        return 0, 0, 0, 0, 0, b"", 0, 0, 0, {}, [], []
    buffer = buffer[pos_value:]
    total_length += pos_value

    pos_headers, headers_count = decode_varint(buffer)
    if pos_headers == 0:
        return 0, 0, 0, 0, 0, b"", 0, 0, 0, {}, [], []
    buffer = buffer[pos_headers:]
    total_length += pos_headers

    headers: list[str] = []
    for _ in range(headers_count):
        logging.error(
            "unhandled header (expected count %d)\nbuffer %s",
            headers_count,
            buffer[:20].hex(" "),
        )
        return (
            pos_length + length,
            length,
            attr,
            ts_delta,
            offset_delta,
            key,
            frame_version,
            record_type,
            record_version,
            record_data,
            fields,
            headers,
        )

    return (
        total_length,
        length,
        attr,
        ts_delta,
        offset_delta,
        key,
        frame_version,
        record_type,
        record_version,
        record_data,
        fields,
        headers,
    )
