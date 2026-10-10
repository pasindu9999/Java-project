#!/usr/bin/env bash
# Container healthcheck: healthy only when /actuator/health answers 200 (Actuator answers 503 when DOWN).
# The JRE base image has neither curl nor wget, so this speaks plain HTTP over bash's /dev/tcp instead of
# adding a package to every image.
#   usage: healthcheck <port>
set -euo pipefail
port="${1:?usage: healthcheck <port>}"

exec 3<>"/dev/tcp/127.0.0.1/${port}"
printf 'GET /actuator/health HTTP/1.0\r\nHost: localhost\r\n\r\n' >&3
IFS= read -r status_line <&3
[[ "${status_line}" == *" 200 "* ]]
