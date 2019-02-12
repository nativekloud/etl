FROM openjdk:10-jre
RUN mkdir -p /app /app/resources
WORKDIR /app
COPY etl .
CMD  [./etl]
