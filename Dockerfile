# Use the official gradle image as a base image
FROM gradle:jdk17 AS build

# Allow access
EXPOSE 7474
EXPOSE 7687

# Set the working directory
WORKDIR /home/gradle/src

# Copy the source code into the container
RUN git clone https://github.com/tum-gis/3dcitykg

# Set the working directory
WORKDIR /home/gradle/src/3dcitykg

# Run the application using Gradle
CMD ["gradle", "run"]
