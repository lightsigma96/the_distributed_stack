from typing import List, Tuple
import argparse
import socket


def parse_server_spec(spec: str) -> Tuple[str, str]:
    """Parse CONTROL_ADDRESS=CAMERA_UDP_ADDRESS."""
    if "=" not in spec:
        raise ValueError(
            f"invalid server spec {spec!r}; expected CONTROL_ADDRESS=CAMERA_UDP_ADDRESS"
        )

    control_address, camera_address = spec.split("=", 1)
    if not control_address or not camera_address:
        raise ValueError(
            f"invalid server spec {spec!r}; both control and camera addresses are required"
        )
    return control_address, camera_address


def send_request(server_specs: List[str]) -> None:
    parsed = [parse_server_spec(spec) for spec in server_specs]
    connections: List[Tuple[socket.socket, str]] = []

    try:
        # Establish all CLI connections before sending configuration. This
        # prevents early peer dials from racing ahead of later CLI requests.
        for control_address, _ in parsed:
            host, port_text = control_address.rsplit(":", 1)
            connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            connection.settimeout(5)
            connection.connect((host, int(port_text)))
            connections.append((connection, control_address))

        for index, (connection, _) in enumerate(connections):
            lines = ["CONFIG"]
            for peer_index, (peer_control, peer_camera) in enumerate(parsed):
                if peer_index == index:
                    continue
                lines.append(f"{peer_control}|{peer_camera}")
            lines.append("END")
            message = "\n".join(lines) + "\n"
            connection.sendall(message.encode("utf-8"))

        for connection, control_address in connections:
            try:
                response = connection.recv(1024).decode("utf-8").strip()
                print(f"{control_address}: {response or 'configuration sent'}")
            except socket.timeout:
                print(f"{control_address}: timed out waiting for confirmation")
    finally:
        for connection, _ in connections:
            connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="cli-tool",
        description="Configure the server cluster and advertise each server's camera UDP endpoint",
    )
    parser.add_argument(
        "servers",
        nargs="+",
        help="server endpoints as CONTROL_ADDRESS=CAMERA_UDP_ADDRESS",
    )
    args = parser.parse_args()

    try:
        send_request(args.servers)
    except (OSError, ValueError) as err:
        parser.error(str(err))


if __name__ == "__main__":
    main()
