# Command Line Interface

## Docker

```bash
# Build container
docker build --no-cache -t 3dcitykg .

# For experimenting with the docker image
docker run -it --rm -p7474:7474 -p7687:7687 3dcitykg
# OR Run detached with name and tail logs
docker run -d --name 3dcitykg -p7474:7474 -p7687:7687 3dcitykg ; docker logs --tail 50 -f 3dcitykg

# For production, it is recommended to build the JAR and include it in the ENTRYPOINT of the Dockerfile
# The JAR file is located in build/libs
# .\gradlew shadowJar

# Tag and push the image to Docker Hub
#docker tag 3dcitykg sonnguyentum/3dcitykg:1.0.0
#docker push sonnguyentum/3dcitykg:1.0.0
docker tag 3dcitykg tumgis/3dcitykg:1.0.0
docker push tumgis/3dcitykg:1.0.0

# MULTI_ARCH BUILD (for Apple M1/M2 and AMD64)

# Enable Docker Buildx (if not already)
docker buildx create --use
docker buildx inspect --bootstrap

# Build multi-arch image
docker buildx build \
  --no-cache \
  --platform linux/amd64,linux/arm64 \
  -t tumgis/3dcitykg:1.0.0 \
  .

# Test local
docker run --rm -t -p7474:7474 -p7687:7687 tumgis/3dcitykg:1.0.0

# Push multi-arch image to Docker Hub
docker buildx build \
    --no-cache \
    --platform linux/amd64,linux/arm64 \
    -t tumgis/3dcitykg:1.0.0 \
    --push \
    .
```
