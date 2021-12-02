FROM openjdk:8u151-jdk-alpine3.7

# Copy resources
COPY target/end-term-0.0.1-SNAPSHOT-jar-with-dependencies.jar end-term.jar