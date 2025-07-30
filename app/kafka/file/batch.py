import logging
import struct

from .record import decode_record


def decode_batch(
    buffer: bytes,
) -> tuple[
    int,
    dict[
        str,
        int | list[dict[str, str | int | bytes | list[int] | list[bytes] | list[str]]],
    ],
]:
    batch_data = {}

    logging.debug("pre header %s", buffer[:25].hex(" "))
    header_format = ">QIIBI"
    header_length = struct.calcsize(header_format)
    if len(buffer) < header_length:
        return 0, {}
    record_batch, batch_length, epoch, version, crc = struct.unpack(
        header_format, buffer[:header_length]
    )
    buffer = buffer[header_length:]

    header_data = {
        "record_batch": record_batch,
        "batch_length": batch_length,
        "epoch": epoch,
        "version": version,
        "crc": crc,
    }
    batch_data.update(header_data)

    logging.info(
        "Meta Header: %d %s",
        header_length,
        header_data,
    )

    logging.debug("pre info %s", buffer[:45].hex(" "))
    info_format = ">HIQQqhiI"
    info_length = struct.calcsize(info_format)
    if len(buffer) < info_length:
        return 0, batch_data
    attr, offset, ts_base, ts_max, pid, pepoch, seq, records_length = struct.unpack(
        info_format, buffer[:info_length]
    )
    buffer = buffer[info_length:]

    info_data = {
        "attr": attr,
        "offset": offset,
        "ts_base": ts_base,
        "ts_max": ts_max,
        "pid": pid,
        "pepoch": pepoch,
        "seq": seq,
        "records_length": records_length,
    }
    batch_data.update(info_data)

    logging.info(
        "Meta Info: %d %s",
        info_length,
        info_data,
    )

    batch_data["records"] = []
    pos_records = 0
    for _ in range(records_length):
        pos_record, record_info = decode_record(buffer)
        if pos_record == 0:
            return 0, batch_data
        logging.info("Meta Record: %d %s", pos_record, record_info)
        buffer = buffer[pos_record:]
        batch_data["records"].append(record_info)
        pos_records += pos_record

    return header_length + info_length + pos_records, batch_data
