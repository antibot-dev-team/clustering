import re
import datetime
import time
import json
from collections import defaultdict
from time import mktime
import argparse


def parse_interval(log_name: str, interval: int, limit=0) -> None:
    """
    Create JSON file in ./dumps directory:
    1) log_clients_{interval duration}s_{lines parsed}.json
        -- Requests per intervals of given duration.
        Has a form like: {"IP:UA": [req. per first interval, req. per second interval, ...], ...}
        If a client did not make any requests in interval, RPI for this interval is not recorded.

    :param log_name: Name of file with logs
    :param interval: Duration of interval
    :param limit: Amount of lines to consider
    :return: None
    """

    clients_reqs = defaultdict(list)
    client_session_reqs = defaultdict(list)

    with open(log_name, "r") as log_file:
        i = 0
        for line in log_file:           # парсим в словарь: IP/UA : [[session_1], [session_2], ...]
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

            if len(client_session_reqs[client]) > 0 and ts - client_session_reqs[client][-1] > 30 * 60:
                clients_reqs[client].append(client_session_reqs[client])
                client_session_reqs[client].clear()

            client_session_reqs[client].append(ts)

        for client, ts in client_session_reqs.items():    # заносим в словарь последнюю сессию
            if len(client_session_reqs) > 0:
                clients_reqs[client].append(client_session_reqs[client])

        for client, requests in clients_reqs.items():  # изменяем содержимое session_i: ts -> средняя скорость запросов за 30с
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
                        session_speed.append(interval_reqs / (ts - start_ts) if ts - start_ts > 0 else interval_reqs)
                request_speed.append(session_speed)
            clients_reqs[client] = request_speed

    with open(
        "./dumps/log_clients_{}s_{}k.json".format(interval, limit // 1000), "w"
    ) as outfile:
        json.dump(clients_reqs, outfile, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Calculate requests per interval and dump to json file."
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
