docker login   # inserisci user/password Docker Hub

# Crea un builder multi-arch (una volta sola)
docker buildx create --name multiarch --use
docker buildx inspect --bootstrap

docker buildx build \
  --platform linux/arm64 \
  -t edoebzd/private:simulator \
  --push \
  ./simulator

docker buildx build \
  --platform linux/arm64 \
  -t edoebzd/private:infra\
  --push \
  ./infra

docker buildx build \
  --platform linux/arm64 \
  -t edoebzd/private:ml-detector \
  --push \
  ./ml-detector

docker buildx build \
  --platform linux/arm64 \
  -t edoebzd/private:crew-agent \
  --push \
  ./crew-agent

docker buildx build \
  --platform linux/arm64 \
  -t edoebzd/private:grafana \
  --push \
  ./grafana