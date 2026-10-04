from typing import List
import socket
import argparse

def send_request(servers_list : List[str]):

    for c in servers_list:
        message : str = ""
        for s in servers_list:
            if s == c:
                continue
            message += f"{s}\r\n"

        message += f"\r\n\r\n"
       
        TCP_IP = c.split(":")[0]
        TCP_PORT = int(c.split(":")[1])


        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((TCP_IP, TCP_PORT))
            s.sendall(message.encode("utf-8"))
        print(message)

def main():
    parser = argparse.ArgumentParser(
                    prog='cli-tool',
                    description='Sends requests to servers to help them be part of cluster')

    parser.add_argument('servers', nargs='+', help='Addresses of servers')
    servers_list : List[str] = []

    for a in parser.parse_args().servers:
        servers_list.append(a)

    send_request(servers_list)

if __name__ == "__main__":
    main()
else:
    print("script supposed to be run indenpendently")
