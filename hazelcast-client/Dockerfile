FROM openjdk:8-jre

# Copy from Maven build output
ARG JAR_FILE
ADD target/${JAR_FILE} application.jar

# Environment variable, default to empty string
ENV K8S ""

ENTRYPOINT exec java -Dserver.port=8080 -Dk8s=$K8S -jar application.jar
