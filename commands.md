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
```
