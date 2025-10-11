#!/usr/bin/env python
import socket
import sys
import time


def wait_for_dns(hosts):
    """
    Waits for a list of hostnames to be resolvable.
    """
    if not hosts:
        print("wait-for-dns: No hosts to check. Exiting.", file=sys.stderr)
        return

    for host in hosts:
        print(f"wait-for-dns: Waiting for DNS resolution for '{host}'...")
        while True:
            try:
                socket.gethostbyname(host)
                print(f"wait-for-dns: Host '{host}' is resolvable.")
                break  # Успех, переходим к следующему хосту
            except socket.gaierror:
                print(f"wait-for-dns: Host '{host}' is not yet resolvable, retrying in 2 seconds...", file=sys.stderr)
                time.sleep(2)

if __name__ == "__main__":
    # Получаем список хостов из аргументов командной строки, пропуская имя самого скрипта.
    wait_for_dns(sys.argv[1:])