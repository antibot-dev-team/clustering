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

            if ip not in ip_to_ua:
                ip_to_ua[ip] = set()
            ip_to_ua[ip].add(ua)

    return sum(len(ua) > 1 for ua in ip_to_ua.values())
