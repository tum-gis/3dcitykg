# Use the official Gradle image as a base for building
FROM gradle:9.0.0-jdk17 AS build

# Allow access to required ports
EXPOSE 7474
EXPOSE 7687

# Install Neo4j and dependencies
RUN wget -O - https://debian.neo4j.com/neotechnology.gpg.key | gpg --dearmor -o /etc/apt/keyrings/neotechnology.gpg && \
    echo 'deb [signed-by=/etc/apt/keyrings/neotechnology.gpg] https://debian.neo4j.com stable 5' | tee -a /etc/apt/sources.list.d/neo4j.list && \
    apt-get update && \
    apt-get -y install neo4j

# Find installed Neo4j version and fetch matching APOC release
RUN apt-get -y install jq
RUN NEO4J_VERSION=$(neo4j --version) && \
    echo "Detected Neo4j version: $NEO4J_VERSION" && \
    APOC_URL=$(curl -s https://api.github.com/repos/neo4j/apoc/releases \
        | jq -r --arg v "$NEO4J_VERSION" '.[] | select(.tag_name==$v) | .assets[] | select(.name|test("core.jar$")) | .browser_download_url' | head -n 1) && \
    echo "Downloading APOC from: $APOC_URL" && \
    wget -O /var/lib/neo4j/plugins/apoc.jar "$APOC_URL"

# Set the working directory
WORKDIR /home/gradle/src

# Clone the repository
RUN git clone https://github.com/tum-gis/3dcitykg
# RUN git clone --branch v1.1.0 --depth 1 https://github.com/tum-gis/3dcitykg

# Change to project directory
WORKDIR /home/gradle/src/3dcitykg

# Cache Gradle dependencies
RUN gradle dependencies --no-daemon || true

# Build the application
COPY gradlew gradle/ /home/gradle/src/3dcitykg/
RUN ./gradlew build --no-daemon

# Replace the default Neo4j configuration
COPY config/neo4j.conf /etc/neo4j/neo4j.conf

# Run the application and start Neo4j
CMD gradle run && neo4j console
