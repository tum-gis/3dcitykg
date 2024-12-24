# Use the official Gradle image as a base for building
FROM gradle:jdk17 AS build

# Allow access to required ports
EXPOSE 7474
EXPOSE 7687

# Set the working directory
WORKDIR /home/gradle/src

# Clone the repository
RUN git clone https://github.com/tum-gis/3dcitykg

# Change to project directory
WORKDIR /home/gradle/src/3dcitykg

# Create Neo4j instance
RUN gradle run

# Install Neo4j
RUN wget -O - https://debian.neo4j.com/neotechnology.gpg.key | gpg --dearmor -o /etc/apt/keyrings/neotechnology.gpg
RUN echo 'deb [signed-by=/etc/apt/keyrings/neotechnology.gpg] https://debian.neo4j.com stable latest' | tee -a /etc/apt/sources.list.d/neo4j.list
RUN apt-get update
RUN apt-get -y install neo4j

# Replace the default Neo4j configuration
COPY lib/config/neo4j.conf /etc/neo4j/neo4j.conf

# Start Neo4j
CMD ["neo4j", "console"]
