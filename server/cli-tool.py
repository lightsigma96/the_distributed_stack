from typing import List

import requests
import argparse

def send_request(servers_list : List[str]):


def main():
    parser = argparse.ArgumentParser(
                    prog='cli-tool',
                    description='Sends requests to servers to help them be part of cluster')

    parser.add_argument('servers', nargs='+', help='Addresses of servers')

    for a in parser.parse_args().servers:
        print(a)

if __name__ == "__main__":
    main()
