import re
import datetime
import time
import os
import argparse

import pandas as pd

from collections import defaultdict


def parse_interval(log_name: str, interval: int, limit=0) -> None:
    """
    Create or append requests per interval to ./dumps/requests.csv file

    :param log_name: Name of file with logs
    :param interval: Duration of interval
    :param limit: Amount of lines to consider
    :return: None
    """

    clients_reqs = defaultdict(list)
    client_session_reqs = defaultdict(list)

    with open(log_name, "r") as log_file:
        i = 0
        for line in log_file:  # Parse to dict: IP/UA : [[session_1], [session_2], ...]
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

            if (
                len(client_session_reqs[client]) > 0
                and ts - client_session_reqs[client][-1] > 30 * 60
            ):
                clients_reqs[client].append(client_session_reqs[client])
                client_session_reqs[client].clear()

            client_session_reqs[client].append(ts)

        for (
            client,
            ts,
        ) in client_session_reqs.items():  # Append last session to the dict
            if len(client_session_reqs) > 0:
                clients_reqs[client].append(client_session_reqs[client])

        for (
            client,
            requests,
        ) in clients_reqs.items():  # Calculate RPI per each interval
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
            clients_reqs[client] = request_speed

    # Write to csv
    ip = []
    ua = []
    for key in clients_reqs.keys():
        ip_ua = key.split(":")
        ip.append(ip_ua[0])
        ua.append(ip_ua[1])

    frame = defaultdict(list)
    for client, sessions in clients_reqs.items():
        ip_ua = client.split(":")
        for i in range(len(sessions)):
            frame["IP"].append(ip_ua[0])
            frame["UA"].append(ip_ua[1])
            frame["Session"].append(i + 1)
            frame[f"RPI{interval}"].append(sessions[i])

    df = pd.DataFrame(frame)

    if os.path.isfile("./dumps/requests.csv"):
        df_old = pd.read_csv("./dumps/requests.csv")
        if f"RPI{interval}" in df_old.columns:
            df_old = df_old.drop(f"RPI{interval}", axis=1)
        df = df.merge(df_old, on=["IP", "UA", "Session"])

    df.to_csv("./dumps/requests.csv", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Calculate requests per interval and dump to csv file."
    )
    parser.add_argument(
        "--interval",
        metavar="t",
        type=int,
        help="Interval duration in seconds.",
        default=30,
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

    parse_interval(args.log, args.interval, args.limit)
