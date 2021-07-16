def unique_ip(log_name: str, lines=10_000) -> int:
    """
    Parse log file and count requests with unique IP addresses.
    :param log_name: Name of file with logs
    :param lines: How many lines to consider
    :return: Amount of requests with unique IP addresses
    """
    ips = set()
    with open(log_name, "r") as log_file:
        for _ in range(lines):
            line = log_file.readline()
            ip = line[: line.find("-") - 1]
            ips.add(ip)
    return len(ips)


def ip_with_multiple_ua(log_name: str, lines=10_000) -> int:
    """
    Parse log file and count requests with same IP but different User-Agents.
    :param log_name: Name of file with logs
    :param lines: How many lines to consider
    :return: Amount of IP addresses with different User-Agents
    """
    ip_to_ua = dict()
    with open(log_name, "r") as log_file:
        for _ in range(lines):
            line = log_file.readline()

            ip = line[: line.find("-") - 1]
            ua = line.split('"')[5]

            if ip not in ip_to_ua:
                ip_to_ua[ip] = set()
            ip_to_ua[ip].add(ua)
    return sum(len(ua) > 1 for ua in ip_to_ua.values())
