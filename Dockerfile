# syntax=docker/dockerfile:1.7

FROM eclipse-temurin:24-jdk AS build

WORKDIR /workspace
ARG APP_TASK=:coordinator-server:bootJar
ARG APP_LIB_DIR=coordinator-server/build/libs

COPY gradlew gradlew.bat settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle ./gradle

COPY coordinator-server ./coordinator-server
COPY redisstream-spring-boot-starter ./redisstream-spring-boot-starter
COPY samples ./samples

RUN --mount=type=cache,target=/root/.gradle \
    if [ "$APP_TASK" = ":coordinator-server:bootJar" ]; then \
      ./gradlew :coordinator-server:bootJar --no-daemon; \
    else \
      ./gradlew "$APP_TASK" --no-daemon; \
    fi \
    && find "$APP_LIB_DIR" -maxdepth 1 -type f -name "*.jar" ! -name "*-plain.jar" \
      -exec cp {} /workspace/application.jar \;

FROM eclipse-temurin:24-jre AS runtime

WORKDIR /app

RUN groupadd --system redisstream \
    && useradd --system --gid redisstream --home-dir /app --shell /usr/sbin/nologin redisstream

COPY --from=build /workspace/application.jar /app/application.jar

ENV JAVA_OPTS="" \
    COORDINATOR_STORE_TYPE=redis

USER redisstream

EXPOSE 8080

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/application.jar"]
