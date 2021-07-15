import datetime
import re
import json


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

            ts = re.findall(r"\[(\d+/\w+/\d+:\d+:\d+:\d+ [+-]?\d+)\]", line)
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

    with open("./dumps/log_clients_{}s.json".format(interval), "w") as result:
        json.dump(clients_rpi, result)


if __name__ == "__main__":
    limit = 1_000_000
    log = "./access.log"

    # unique = unique_ip(log, limit)
    # print("Unique IPs for the first {} entries: {}".format(limit, unique))
    #
    # multiple_ua = ip_with_multiple_ua(log, limit)
    # print(
    #     "Count of IPs with multiple User-Agent for the first {} entries: {}".format(
    #         limit, multiple_ua
    #     )
    # )

    parse_interval(log, 30, limit)
