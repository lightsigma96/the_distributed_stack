# Start Kafka
./start_kafka.sh

# Start application servers. The camera address defaults to the host in
# -server_address plus the UDP port, but can be provided explicitly.
./bin/server -server_address localhost:7001 -cam_addr 7101 -assign_listen_addr localhost:7201
./bin/server -server_address localhost:7002 -cam_addr 7102 -assign_listen_addr localhost:7202

# Configure the cluster. Each entry is control-address=camera-udp-address.
python3 server/cli-tool.py localhost:7001=localhost:7101 localhost:7002=localhost:7102

# Camera discovers a target server from one or more assignment endpoints.
./bin/camera -assign_addr localhost:7201,localhost:7202

# Direct UDP mode is still available for tests:
# ./bin/camera -server_addr localhost:7101
