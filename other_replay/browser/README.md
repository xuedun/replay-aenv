# build

```bash
docker build -t agent-browser-arm64 --build-arg HTTP_PROXY=http://90.255.222.172:8888   --build-arg HTTPS_PROXY=http://90.255.222.172:8888 .
```

# run container

```bash
docker run -d --shm-size=1g --name replay-test-env agent-browser-arm64
```

# start jucie shop

```bash
docker pull bkimminich/juice-shop
docker run -d --name juice-shop -p 4000:3000 bkimminich/juice-shop
```

# verify

```bash
python3 verify_replay.py --container replay-test-env --target-url http://90.91.181.189:4000
```