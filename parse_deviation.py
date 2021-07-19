import datetime
import argparse
import re
import json
import time
import math

import numpy as np


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
    clients_diff = dict()
    for client, stamps in clients_reqs.items():
        clients_diff[client] = [
            abs(stamps[i + 1] - stamps[i]) for i in range(len(stamps) - 1)
        ]

    # TODO: fix naming when limit = 0
    with open("dumps/log_clients_diff_{}k.json".format(limit // 1000), "w") as outfile:
        json.dump(clients_diff, outfile, indent=4)

    clients_mean = dict()
    for client, stamps in clients_diff.items():
        clients_mean[client] = sum(stamps) / len(stamps) if len(stamps) > 0 else None

    with open("dumps/log_clients_mean_{}k.json".format(limit // 1000), "w") as outfile:
        json.dump(clients_mean, outfile, indent=4)

    # Mean deviation for the request:
    # [6, 4, 5] => [abs(4-6), abs(5-4)] => [2, 1] => (2+1)/2 => 1.5 mean deviation

    # Calculate mean deviation
    clients_deviation = dict()
    for client, stamps in clients_diff.items():
        clients_deviation[client] = [
            np.power(stamps[i] - clients_mean[client], 2) for i in range(len(stamps))
        ]

    for client, stamps in clients_deviation.items():
        clients_deviation[client] = (
            math.sqrt(sum(stamps) / (len(stamps) - 1)) if len(stamps) - 1 > 0 else 0
        )

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
