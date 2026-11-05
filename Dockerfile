# syntax=docker/dockerfile:1.7
# ---- build stage -----------------------------------------------------------
FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /workspace
COPY .mvn .mvn
COPY mvnw pom.xml ./
RUN --mount=type=cache,target=/root/.m2 ./mvnw -q -B dependency:go-offline
COPY src src
RUN --mount=type=cache,target=/root/.m2 ./mvnw -q -B -DskipTests package \
    && java -Djarmode=tools -jar target/*.jar extract --layers --destination extracted

# ---- runtime stage ---------------------------------------------------------
FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S -g 1000 app && adduser -S -u 1000 -G app app \
    && mkdir -p /data/uploads && chown -R app:app /data
WORKDIR /app
# Layered copy: dependencies change rarely, application code often.
COPY --from=build --chown=app:app /workspace/extracted/dependencies/ ./
COPY --from=build --chown=app:app /workspace/extracted/spring-boot-loader/ ./
COPY --from=build --chown=app:app /workspace/extracted/snapshot-dependencies/ ./
COPY --from=build --chown=app:app /workspace/extracted/application/ ./
USER app
ENV STORAGE_ROOT=/data/uploads \
    SPRING_PROFILES_ACTIVE=json \
    JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=70 -XX:+ExitOnOutOfMemoryError -Djava.security.egd=file:/dev/./urandom"
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=40s --retries=5 \
    CMD wget -qO- http://localhost:8080/health/live || exit 1
ENTRYPOINT ["java", "-jar", "transaction-reconciliation-service-0.1.0-SNAPSHOT.jar"]
