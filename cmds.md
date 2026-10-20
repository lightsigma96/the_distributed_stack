./bin/ml

./bin/server -server_address localhost:7001 -cam_addr 7101 

./bin/server -server_address localhost:7002 -cam_addr 7102

python3 server/cli-tool.py localhost:7001 localhost:7002

./bin/camera -server_addr localhost:7101 

./bin/camera -server_addr localhost:7102