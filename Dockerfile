# syntax=docker/dockerfile:1
#
# One recipe for all three services:  docker build --build-arg SERVICE=order-service -t orderflow/order-service .
# (docker compose --profile apps up --build passes SERVICE for each one.)
#
# Multi-stage: the JDK and Maven exist only in the build stage. The runtime image is a JRE plus the app,
# split into layers that change at different rates, so a code change rebuilds and pushes only the top layer.

ARG SERVICE

FROM eclipse-temurin:25-jdk AS build
ARG SERVICE
WORKDIR /src
COPY . .
# The Maven repository (and the wrapper's Maven download) is a BuildKit cache, shared by all three builds;
# "locked" stops parallel builds from writing it at the same time. Tests are skipped: the ITs need Docker
# themselves (Testcontainers) and run in CI. -DskipTests still builds the platform-messaging test-jar,
# which the service's test-scoped dependencies need to resolve.
RUN --mount=type=cache,target=/root/.m2,sharing=locked \
    ./mvnw -B -ntp -q -pl "${SERVICE}" -am package -DskipTests
RUN java -Djarmode=tools -jar "${SERVICE}"/target/"${SERVICE}"-*-exec.jar \
        extract --layers --launcher --destination /layers

FROM eclipse-temurin:25-jre
RUN useradd --system --uid 10001 --no-create-home app
WORKDIR /app
# Least to most frequently changing.
COPY --from=build /layers/dependencies/ ./
COPY --from=build /layers/spring-boot-loader/ ./
COPY --from=build /layers/snapshot-dependencies/ ./
COPY --from=build /layers/application/ ./
COPY --chmod=755 docker/healthcheck.sh /usr/local/bin/healthcheck
USER app
# Size the heap from the container's memory limit, not the host's.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75"
ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
