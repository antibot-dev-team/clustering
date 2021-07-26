import datetime
import argparse
import re
import math
import os

from collections import defaultdict

import pandas as pd


def parse_deviation(log_name: str, interval: int, limit=0) -> None:
    """
    Create ./dumps/requests.csv file with:
    1) Time difference between requests in each session
    2) Mean value of the differences between requests in each session
    3) Mean deviation of time difference between requests in each session
    4) Requests per interval for each session

    :param interval: Duration of intervals for calculating RPI
    :param log_name: Name of file with logs
    :param limit: Amount of lines to consider
    :return: None
    """

    # Parse client requests
    clients_reqs = defaultdict(list)  # Will contain all sessions
    clients_session_reqs = defaultdict(list)  # Temp dict for filling sessions
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
            ts = int(ts.timestamp())

            client = "{}:{}".format(ip, ua)
            if (
                    len(clients_session_reqs[client]) > 0
                    and ts - clients_session_reqs[client][-1] >= 30 * 60
            ):
                clients_reqs[client].append(clients_session_reqs[client])
                clients_session_reqs[client] = []

            clients_session_reqs[client].append(ts)

    # Append last session to the dict
    for client, requests in clients_session_reqs.items():
        if len(requests) > 0:
            clients_reqs[client].append(requests)

    # Calculate RPI for each interval
    client_rpi = defaultdict(list)
    for client, requests in clients_reqs.items():
        request_speed = []
        for session in requests:
            session_speed = []
            start_ts = session[0]
            interval_reqs = 0
            for ts in session:
                if ts - start_ts >= interval:
                    session_speed.append(interval_reqs / interval)
                    start_ts = ts
                    interval_reqs = 0
                interval_reqs += 1
                if ts is session[-1]:
                    session_speed.append(interval_reqs / interval)
            request_speed.append(session_speed)
        client_rpi[client] = request_speed

    # Calculate differences between neighbour timestamps
    clients_diff = defaultdict(list)
    for client, sessions in clients_reqs.items():
        session_diffs = []
        for session in sessions:
            for i in range(len(session) - 1):
                diff = session[i + 1] - session[i]
                if diff < 60 * 30:  # less than 30 minutes difference
                    session_diffs.append(diff)
                else:
                    clients_diff[client].append(session_diffs if len(session_diffs) > 0 else None)
                    session_diffs = []

            if len(session_diffs) > 0:  # all timestamps was read but session is not finished
                clients_diff[client].append(session_diffs)
            elif len(session) == 1:  # only one timestamp (diff can't be calculated for this session)
                clients_diff[client].append(None)

    # Calculate mean of differences
    clients_mean = defaultdict(list)
    for client in clients_diff:
        for session in clients_diff[client]:
            if session is not None:
                clients_mean[client].append(sum(session) / len(session))
            else:
                clients_mean[client].append(None)

    # Calculate mean deviation
    clients_deviation = defaultdict(list)
    for client, sessions_diffs in clients_diff.items():
        for i in range(len(sessions_diffs)):
            if sessions_diffs[i] is None:
                clients_deviation[client].append(None)
                continue

            session_deviation = []
            for diff in sessions_diffs[i]:
                session_deviation.append(
                    (diff - clients_mean[client][i]) ** 2
                )
            clients_deviation[client].append(session_deviation)

    for client, sessions_diffs in clients_deviation.items():
        for i in range(len(sessions_diffs)):
            if sessions_diffs[i] is None:
                continue

            sessions_diffs[i] = (
                math.sqrt(
                    sum(sessions_diffs[i]) / (len(sessions_diffs[i]) - 1)
                )
                if len(sessions_diffs[i]) - 1 > 0
                else None
            )

    # Write to csv
    df_rpi = dict_to_df(client_rpi, f"RPI{interval}")
    df_diff = dict_to_df(clients_diff, "Diff")
    df_mean = dict_to_df(clients_mean, "Mean")
    df_deviation = dict_to_df(clients_deviation, "Deviation")

    df = pd.merge(df_rpi, df_diff, on=["IP", "UA", "Session"])
    df = df.merge(df_mean, on=["IP", "UA", "Session"])
    df = df.merge(df_deviation, on=["IP", "UA", "Session"])

    df.to_csv("./dumps/requests.csv", index=False)


def dict_to_df(dictionary: dict, col_name: str) -> pd.DataFrame:
    """
    Create pd.DataFrame from python dict

    :param dictionary: Should have form {"IP:UA": list}
    :param col_name: Name for DataFrame column with values from dict
    :return: pd.DataFrame with 3 columns: IP, UA, col_name
    """

    frame = defaultdict(list)
    for client, sessions in dictionary.items():
        delim_idx = client.find(":")  # 🤡
        ip = client[:delim_idx]
        ua = client[delim_idx + 1:]

        for i in range(len(sessions)):
            frame["IP"].append(ip)
            frame["UA"].append(ua)
            frame["Session"].append(i + 1)
            frame[col_name].append(sessions[i])

    return pd.DataFrame(frame)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Calculate RPI, request time differences, "
                    "their mean and std deviation and write it to ./dumps/requests.csv"
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
    parser.add_argument(
        "--interval",
        metavar="t",
        type=int,
        help="Interval duration in seconds.",
        default=30,
    )
    args = parser.parse_args()

    parse_deviation(args.log, args.interval, args.limit)
