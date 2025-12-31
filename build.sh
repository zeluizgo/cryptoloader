docker build -t cryptoloader ./
docker network create \
  --driver overlay \
  --attachable \
  crypto-network
docker stack deploy -c docker-compose.yml cryptoloader