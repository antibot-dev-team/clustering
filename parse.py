import datetime
import re
import json
import time


class Defaults:
    limit = 1_000_000
    log = "./access.log"


def unique_ip(log_name: str, lines=10_000) -> int:
    ips = set()
    with open(log_name, "r") as log_file:
        for _ in range(lines):
            line = log_file.readline()
            ip = line[: line.find("-") - 1]
            ips.add(ip)
    return len(ips)


def ip_with_multiple_ua(log_name: str, lines=10_000) -> int:
    ip_to_ua = dict()
    with open(log_name, "r") as log_file:
        for _ in range(lines):
            line = log_file.readline()

            ip = line[: line.find("-") - 1]
            ua = line.split('"')[5]
            # ua = re.findall(r'([^"]{2,})" "-"?$', line)  # -- faulty

            if ip not in ip_to_ua:
                ip_to_ua[ip] = set()
            ip_to_ua[ip].add(ua)

    return sum(len(ua) > 1 for ua in ip_to_ua.values())


def parse_interval(log_name: str, interval: int, limit=0) -> None:
    # "ip:user-agent": [reqs per first interval, ... per second, ...]
    clients_rpi = dict()

    # "ip:user-agent": [timestamp1, timestamp2, timestamp3, ...] -- updated on every interval
    clients_reqs = dict()

    interval_start = None
    with open(log_name, "r") as log_file:
        i = 0
        for line in log_file:
            i += 1
            if limit != 0 and i >= limit:
                break

            ip = line[: line.find("-") - 1]
            ua = line.split('"')[5]

            ts = re.findall(r"\[(\d+/\w+/\d+:\d+:\d+:\d+ [+-]?\d+)]", line)
            ts = datetime.datetime.strptime(
                ts[0], "%d/%b/%Y:%H:%M:%S %z"
            )  # e.g. [22/Jan/2019:06:38:40 +0330]

            client = "{}:{}".format(ip, ua)
            if client not in clients_reqs:
                clients_reqs[client] = 0
                # clients_reqs[client] = list()
            clients_reqs[client] += 1
            # clients_reqs[client].append(ts)

            if interval_start is None:
                interval_start = ts
                continue

            if ts - interval_start >= datetime.timedelta(seconds=interval):
                interval_start = None
                for client in clients_reqs:
                    if client not in clients_rpi:
                        clients_rpi[client] = list()
                    rpi = clients_reqs[client] / interval
                    # rpi = len(clients_reqs[client] / interval
                    clients_rpi[client].append(rpi)

                clients_reqs = dict()  # if 0 rpi records are not needed
                # for client in clients_reqs:  # comment prev, line and uncomment this if needed
                #     clients_reqs[client] = 0

    with open("./dumps/log_clients_{}s.json".format(interval), "w") as outfile:
        json.dump(clients_rpi, outfile, indent=4)


def parse_deviation(log_name: str, limit=0) -> None:
    clients_reqs = dict()

    with open(log_name, "r") as log_file:
        i = 0
        for line in log_file:
            i += 1
            if limit != 0 and i >= limit:
                break

            ip = line[: line.find("-") - 1]
            ua = line.split('"')[5]

            ts = re.findall(r"\[(\d+/\w+/\d+:\d+:\d+:\d+ [+-]?\d+)]", line)
            ts = datetime.datetime.strptime(
                ts[0], "%d/%b/%Y:%H:%M:%S %z"
            )  # e.g. [22/Jan/2019:06:38:40 +0330]
            ts = time.mktime(ts.timetuple())

            client = "{}:{}".format(ip, ua)
            if client not in clients_reqs:
                clients_reqs[client] = list()
            clients_reqs[client].append(ts)

    # Difference between the requests:
    # [10, 16, 20, 25] => [16-10, 20-16, 25-20] => [6, 4, 5] => 5 sec. average difference between requests

    # Calculate differences between neighbour timestamps
    clients_diff = dict()
    for client, stamps in clients_reqs.items():
        clients_diff[client] = [
            abs(stamps[i + 1] - stamps[i]) for i in range(len(stamps) - 1)
        ]

    with open("dumps/log_clients_diff.json", "w") as outfile:
        json.dump(clients_diff, outfile, indent=4)

    clients_mean = dict()
    for client, stamps in clients_diff.items():
        clients_mean[client] = sum(stamps) / len(stamps) if len(stamps) > 0 else []

    with open("dumps/log_clients_mean.json", "w") as outfile:
        json.dump(clients_mean, outfile, indent=4)

    # Average deviation for the request:
    # [6, 4, 5] => [abs(4-6), abs(5-4)] => [2, 1] => (2+1)/2 => 1.5 average deviation between requests

    clients_deviation = dict()
    for client, stamps in clients_diff.items():
        clients_deviation[client] = [
            abs(stamps[i + 1] - stamps[i]) for i in range(len(stamps) - 1)
        ]

    for client, stamps in clients_deviation.items():
        clients_deviation[client] = sum(stamps) / len(stamps) if len(stamps) > 0 else []

    with open("dumps/log_clients_deviation.json", "w") as outfile:
        json.dump(clients_deviation, outfile, indent=4)


if __name__ == "__main__":
    # unique = unique_ip(Defaults.log, Defaults.limit)
    # print("Unique IPs for the first {} entries: {}".format(Defaults.limit, unique))
    #
    # multiple_ua = ip_with_multiple_ua(Defaults.log, Defaults.limit)
    # print(
    #     "Count of IPs with multiple User-Agent for the first {} entries: {}".format(
    #         Defaults.limit, multiple_ua
    #     )
    # )

    parse_interval(Defaults.log, 30, Defaults.limit)
    parse_deviation(Defaults.log, Defaults.limit)
