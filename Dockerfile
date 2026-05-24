# syntax=docker/dockerfile:1.7

FROM eclipse-temurin:24-jdk AS build

WORKDIR /workspace

COPY gradlew gradlew.bat settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle ./gradle
COPY coordinator-server/build.gradle.kts ./coordinator-server/build.gradle.kts
COPY redisstream-spring-boot-starter/build.gradle.kts ./redisstream-spring-boot-starter/build.gradle.kts

RUN --mount=type=cache,target=/root/.gradle ./gradlew :coordinator-server:dependencies --no-daemon

COPY coordinator-server ./coordinator-server
COPY redisstream-spring-boot-starter ./redisstream-spring-boot-starter

RUN --mount=type=cache,target=/root/.gradle ./gradlew :coordinator-server:bootJar --no-daemon \
    && find coordinator-server/build/libs -maxdepth 1 -type f -name "*.jar" ! -name "*-plain.jar" \
      -exec cp {} /workspace/coordinator-server.jar \;

FROM eclipse-temurin:24-jre AS runtime

WORKDIR /app

RUN groupadd --system redisstream \
    && useradd --system --gid redisstream --home-dir /app --shell /usr/sbin/nologin redisstream

COPY --from=build /workspace/coordinator-server.jar /app/coordinator-server.jar

ENV JAVA_OPTS="" \
    COORDINATOR_STORE_TYPE=redis

USER redisstream

EXPOSE 8080

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/coordinator-server.jar"]
