import datetime
import argparse
import re
import json
import math
import time
from collections import defaultdict


def parse_deviation(log_name: str, limit=0) -> None:
    """
    Create JSON files in ./dumps directory:
    1) log_clients_diff_{lines parsed}.json
        -- Time difference between requests

    2) log_clients_mean_{lines parsed}.json
        -- Mean value of the differences between requests

    3) log_clients_deviation_{lines parsed}.json
        -- Mean deviation of time difference between requests

    :param log_name: Name of file with logs
    :param limit: Amount of lines to consider
    :return: None
    """
    clients_reqs = dict()

    with open(log_name, "r") as log_file:
        i = 0
        for line in log_file:
            i += 1
            if limit != 0 and i >= limit:
                break

            ip = line[: line.find("-") - 1]
            ua = line.split('"')[5]  # User-Agent goes after 5-th \"

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

    clients_diff = defaultdict(list)
    for client, req_time in clients_reqs.items():
        session_req = []
        for i in range(len(req_time)-1):
            diff = req_time[i + 1] - req_time[i]
            if diff < 60 * 30:  # разница между запросами менее 30-и минут
                session_req.append(diff)
            else:
                clients_diff[client].append(session_req)
                session_req.clear()
        if len(session_req) > 0:
            clients_diff[client].append(session_req)
            
    with open("dumps/log_clients_diff_{}k.json".format(limit // 1000), "w") as outfile:
        json.dump(clients_diff, outfile, indent=4)

    clients_mean = defaultdict(list)
    for client in clients_diff:
        for session in clients_diff[client]:
            clients_mean[client].append(sum(session) / len(session) if len(session) > 0 else None)

    with open("dumps/log_clients_mean_{}k.json".format(limit // 1000), "w") as outfile:
        json.dump(clients_mean, outfile, indent=4)

    # Calculate mean deviation
    clients_deviation = defaultdict(list)
    for client, sessions in clients_diff.items():
        for session_numb in range(len(sessions)):
            session_deviation = []
            for timestamp in sessions[session_numb]:
                session_deviation.append((timestamp - clients_mean[client][session_numb]) ** 2)
            clients_deviation[client].append(session_deviation)

    for client, sessions in clients_deviation.items():
        for session_numb in range(len(sessions)):
            sessions[session_numb] = math.sqrt(sum(sessions[session_numb]) / (len(sessions[session_numb]) - 1)) \
                if len(sessions[session_numb]) - 1 > 0 else None

    with open(
        "dumps/log_clients_deviation_{}k.json".format(limit // 1000), "w"
    ) as outfile:
        json.dump(clients_deviation, outfile, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Calculate requests per interval and dump to json file."
    )
    parser.add_argument(
        "--log", metavar="f", type=str, help="Log file", default="./access.log"
    )
    parser.add_argument(
        "--limit",
        metavar="l",
        type=int,
        help="Parse specified amount of lines.",
        default=100_000,
    )
    args = parser.parse_args()

    parse_deviation(args.log, args.limit)
