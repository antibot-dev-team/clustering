import datetime
import argparse
import re
import math
import time
import os

from collections import defaultdict

import pandas as pd


def parse_deviation(log_name: str, limit=0) -> None:
    """
    Create or append columns to ./dumps/requests.csv file:
    1) Time difference between requests
    2) Mean value of the differences between requests
    3) Mean deviation of time difference between requests

    :param log_name: Name of file with logs
    :param limit: Amount of lines to consider
    :return: None
    """

    # Parse client requests
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

    # Calculate differences between neighbour timestamps
    clients_diff = defaultdict(list)
    for client, req_time in clients_reqs.items():
        session_req = []
        for i in range(len(req_time) - 1):
            diff = req_time[i + 1] - req_time[i]
            if diff < 60 * 30:  # 30 minutes difference
                session_req.append(diff)
            else:
                clients_diff[client].append(session_req)
                session_req.clear()
        if len(session_req) > 0:
            clients_diff[client].append(session_req)

    # Calculate mean of differences
    clients_mean = defaultdict(list)
    for client in clients_diff:
        for session in clients_diff[client]:
            clients_mean[client].append(
                sum(session) / len(session) if len(session) > 0 else None
            )

    # Calculate mean deviation
    clients_deviation = defaultdict(list)
    for client, sessions in clients_diff.items():
        for session_numb in range(len(sessions)):
            session_deviation = []
            for timestamp in sessions[session_numb]:
                session_deviation.append(
                    (timestamp - clients_mean[client][session_numb]) ** 2
                )
            clients_deviation[client].append(session_deviation)

    for client, sessions in clients_deviation.items():
        for session_numb in range(len(sessions)):
            sessions[session_numb] = (
                math.sqrt(
                    sum(sessions[session_numb]) / (len(sessions[session_numb]) - 1)
                )
                if len(sessions[session_numb]) - 1 > 0
                else None
            )

    # Write to csv
    df_diff = dict_to_df(clients_diff, "Diff")
    df_mean = dict_to_df(clients_mean, "Mean")
    df_deviation = dict_to_df(clients_deviation, "Deviation")

    df = pd.merge(df_diff, df_mean, on=["IP", "UA"])
    df = df.merge(df_deviation, on=["IP", "UA"])

    if os.path.isfile("./dumps/requests.csv"):
        df_old = pd.read_csv("./dumps/requests.csv")
        if "Diff" in df_old.columns:
            df_old = df_old.drop("Diff", axis=1)

        if "Mean" in df_old.columns:
            df_old = df_old.drop("Mean", axis=1)

        if "Deviation" in df_old.columns:
            df_old = df_old.drop("Deviation", axis=1)
        df = df.merge(df_old, on=["IP", "UA"])

    df.to_csv("./dumps/requests.csv", index=False)


def dict_to_df(dictionary: dict, col_name: str) -> pd.DataFrame:
    """
    Create pd.DataFrame from python dict
    :param dictionary: Should have form {"IP:UA": value}
    :param col_name: Name for DataFrame column with values from dict
    :return: pd.DataFrame with 3 columns: IP, UA, col_name
    """

    ip = []
    ua = []
    for key in dictionary.keys():
        ip_ua = key.split(":")
        ip.append(ip_ua[0])
        ua.append(ip_ua[1])

    return pd.DataFrame({
            "IP": ip,
            "UA": ua,
            col_name: dictionary.values()
    })


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
