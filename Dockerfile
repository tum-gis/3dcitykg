# Use the official Gradle image as a base for building
FROM gradle:8.12.0-jdk17 AS build

# Allow access to required ports
EXPOSE 7474
EXPOSE 7687

# Install Neo4j and dependencies
RUN wget -O - https://debian.neo4j.com/neotechnology.gpg.key | gpg --dearmor -o /etc/apt/keyrings/neotechnology.gpg && \
    echo 'deb [signed-by=/etc/apt/keyrings/neotechnology.gpg] https://debian.neo4j.com stable latest' | tee -a /etc/apt/sources.list.d/neo4j.list && \
    apt-get update && \
    apt-get -y install neo4j

# Set the working directory
WORKDIR /home/gradle/src

# Clone the repository
RUN git clone https://github.com/tum-gis/3dcitykg

# Change to project directory
WORKDIR /home/gradle/src/3dcitykg

# Cache Gradle dependencies
RUN gradle dependencies --no-daemon || true

# Build the application
RUN gradle build --no-daemon

# Replace the default Neo4j configuration
COPY config/neo4j.conf /etc/neo4j/neo4j.conf

# Run the application and start Neo4j
CMD gradle run && neo4j console
