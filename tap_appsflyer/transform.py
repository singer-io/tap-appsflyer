def af_datetime_str_to_datetime(s):
    return datetime.datetime.strptime(s.strip(), "%Y-%m-%d %H:%M:%S")

def xform_datetime_field(record, field_name):
    record[field_name] = af_datetime_str_to_datetime(record[field_name]).isoformat()


def xform_boolean_field(record, field_name):
    value = record[field_name]
    if value is None:
        return

    if value.lower() == "TRUE".lower():
        record[field_name] = True
    else:
        record[field_name] = False


def xform_empty_strings_to_none(record):
    for key, value in record.items():
        if value == "":
            record[key] = None


def xform_na_strings_to_zero(record):
    for key, value in record.items():
        if value == "N/A":
            record[key] = 0


def xform(record):
    xform_empty_strings_to_none(record)
    xform_boolean_field(record, "wifi")
    xform_boolean_field(record, "is_retargeting")
    return record


def xform_agg(record):
    xform_empty_strings_to_none(record)
    xform_na_strings_to_zero(record)
    return record