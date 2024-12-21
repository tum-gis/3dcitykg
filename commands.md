# Command Line Interface

## Docker

```bash
# Build container
docker build --no-cache -t citykg .

# For experimenting with the docker image
docker run -it --rm -p7474:7474 -p7687:7687 citykg
# OR Run detached with name and tail logs
docker run -d --name citykg -p7474:7474 -p7687:7687 citykg ; docker logs --tail 50 -f citykg

# For production, it is recommended to build the JAR and include it in the ENTRYPOINT of the Dockerfile
# The JAR file is located in build/libs
# .\gradlew shadowJar

# Tag and push the image to Docker Hub
#docker tag citymodel-compare sonnguyentum/citymodel-compare:1.0.0
#docker push sonnguyentum/citymodel-compare:1.0.0
docker tag citykg tumgis/citykg:1.0.0
docker push tumgis/citykg:1.0.0
# OR For dev purposes
docker tag citykg tumgis/citykg:1.0.0-dev
docker push tumgis/citykg:1.0.0-dev
```
