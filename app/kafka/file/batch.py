import logging

from .header import decode_header
from .info import decode_info
from .record import decode_record


def decode_batch(buffer: bytes) -> tuple[int, dict[str, int]]:
    pos_header, record_batch, batch_length, epoch, version, crc = decode_header(buffer)
    if pos_header == 0:
        return 0, {}
    buffer = buffer[pos_header:]
    logging.info(
        "Meta Header: %d %d %d %d %d %s",
        pos_header,
        record_batch,
        batch_length,
        epoch,
        version,
        crc.to_bytes(4).hex(" "),
    )

    pos_info, attr, offset, ts_base, ts_max, pid, pepoch, seq, records_length = (
        decode_info(buffer)
    )
    if pos_info == 0:
        return 0, {}
    buffer = buffer[pos_info:]
    logging.info(
        "Meta Info: %d %d %d %d %d %d %d %d %d",
        pos_info,
        attr,
        offset,
        ts_base,
        ts_max,
        pid,
        pepoch,
        seq,
        records_length,
    )

    records = []
    pos_records = 0
    for _ in range(records_length):
        (
            pos_record,
            length,
            attr,
            ts_delta,
            offset_delta,
            key,
            record_data,
            headers,
        ) = decode_record(buffer)
        if pos_record == 0:
            return 0, {}
        logging.info(
            "Meta Record: %d %d %d %d %d %s %s %s",
            pos_record,
            length,
            attr,
            ts_delta,
            offset_delta,
            key,
            record_data,
            headers,
        )
        buffer = buffer[pos_record:]
        records.append(
            (
                length,
                attr,
                ts_delta,
                offset_delta,
                key,
                record_data,
                headers,
            )
        )
        pos_records += pos_record

    return pos_header + pos_info + pos_records, {
        "header": (record_batch, batch_length, epoch, version, crc),
        "info": (attr, offset, ts_base, ts_max, pid, pepoch, seq, records_length),
        "records": records,
    }
