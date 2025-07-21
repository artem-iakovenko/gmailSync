import re
from datetime import datetime, timedelta
import config
from dateutil import parser
import pytz
import time
output_format = "%Y-%m-%d %H:%M:%S"

utc = pytz.UTC


def array_splitter(all_threads, els_per_list):
    splitted_result = []
    thread_list = []
    for thread_id in all_threads:
        thread_list.append(thread_id)
        if len(thread_list) == els_per_list:
            splitted_result.append(thread_list)
            thread_list = []
    if len(thread_list) > 0:
        splitted_result.append(thread_list)
    return splitted_result


def format_recipient(email_addresses):
    emails_list = []
    for email_address in email_addresses:
        match = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', email_address)
        emails_list.append(match.group(0))
    return ", ".join(emails_list)


def date_formatter(unformatted_date):
    unformatted_date = re.sub(r'\[.*?\]|\(.*?\)', '', unformatted_date).strip()
    parsed_date = parser.parse(unformatted_date)
    if parsed_date.tzinfo is None:
        # local_timezone = pytz.timezone('America/New_York')  # Example of assuming a local timezone, modify as needed
        # parsed_date = local_timezone.localize(parsed_date)
        input("No timezone!!!")
    return parsed_date.astimezone(utc).strftime(output_format)

    # prettified_date = re.sub(r'\s+', ' ', unformatted_date)
    # splitted_date = prettified_date.split(" ")
    # day = int(splitted_date[1])
    # month = config.MONTHS[splitted_date[2]]
    # year = int(splitted_date[3])
    # time = splitted_date[4]
    # zone_id = splitted_date[5]
    # print("TIMEZONE: " + zone_id)
    # splitted_time = time.split(":")
    # hours = int(splitted_time[0])
    # mins = int(splitted_time[1])
    # secs = int(splitted_time[2])
    # current_time_in_utc = datetime(year, month, day, hours, mins, secs)
    # try:
    #     needed_zone_h = 2
    #     needed_zone_m = 0
    #     sign = -1 if zone_id[0] == '-' else 1
    #     zone_hours = int(zone_id[1:3][1] if zone_id[1:3][0] == "0" else zone_id[1:3])
    #     zone_minutes = int(0 if zone_id[4:5] == "00" else zone_id[4:5])
    #     hours_to_add = (sign * zone_hours) - needed_zone_h
    #     minutes_to_add = (sign * zone_minutes) - needed_zone_m
    #     result = current_time_in_utc + timedelta(hours=hours_to_add * -1, minutes=minutes_to_add * -1)
    # except:
    #     result = current_time_in_utc
    # return str(result)




def parts_parser(parts):
    for part in parts:
        yield part
        if 'parts' in part:
            yield from parts_parser(part['parts'])


def parse_gmail_headers(headers):
    try:
        message_subject = [i['value'] for i in headers if i["name"].lower() == "subject"][0].replace("Re: ", "")
    except:
        message_subject = ""
    try:
        message_from = format_recipient(
            [i['value'] for i in headers if i["name"].lower() == "from"][0].split(", "))
    except:
        message_from = ""
    try:
        message_to = format_recipient([i['value'] for i in headers if i["name"].lower() == "to"][0].split(", "))
    except:
        message_to = ""
    try:
        message_cc = format_recipient([i['value'] for i in headers if i["name"].lower() == "cc"][0].split(", "))
    except:
        message_cc = ""
    try:
        unf_date = [i['value'] for i in headers if i["name"].lower() == "date"][0]
        message_date = date_formatter(unf_date)
    except Exception as e:
        print(e)
        message_date = ""
    return [message_from, message_to, message_cc, message_date, message_subject]
