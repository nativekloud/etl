FROM openjdk:13-alpine
RUN mkdir -p /app /app/resources
WORKDIR /app
COPY etl .
CMD  [./etl]
