import re
import datetime
import json
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

    # Will contain: "ip:user-agent": [req. per first interval, req. per second interval, ...],
    clients_rpi = dict()

    # Will contain: "ip:user-agent": int(amount of requests in interval)
    # Also will be wiped on every new interval
    clients_reqs = dict()

    # Used to save the time from which the beginning of the interval is counted
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
            clients_reqs[client] += 1  # Register request

            if interval_start is None:
                interval_start = ts
                continue

            if ts - interval_start >= datetime.timedelta(seconds=interval):
                interval_start = None

                for client in clients_reqs:
                    rpi = clients_reqs[client] / interval
                    if client not in clients_rpi:
                        clients_rpi[client] = list()
                    clients_rpi[client].append(rpi)

                clients_reqs = dict()
                # for client in clients_reqs:  # comment prev. line and uncomment this if need to save RPI=0
                #     clients_reqs[client] = 0

        # Write RPI for the last interval if needed
        if interval_start is not None:
            for client in clients_reqs:
                rpi = clients_reqs[client] / interval
                if client not in clients_rpi:
                    clients_rpi[client] = list()
                clients_rpi[client].append(rpi)

    with open(
        "./dumps/log_clients_{}s_{}k.json".format(interval, limit // 1000), "w"
    ) as outfile:
        json.dump(clients_rpi, outfile, indent=4)


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
        default=500_000,
    )
    args = parser.parse_args()

    parse_interval(args.log, args.interval, args.limit)
