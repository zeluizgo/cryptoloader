docker build --platform linux/arm64 -t cryptoloader ./
docker tag cryptoloader ubuntu-pi-03:5000/cryptoloader
docker push ubuntu-pi-03:5000/cryptoloader

#docker stack deploy -c docker-compose.yml cryptoloader