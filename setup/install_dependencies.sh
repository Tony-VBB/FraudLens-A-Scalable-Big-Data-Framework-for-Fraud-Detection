#!/usr/bin/env bash
set -euo pipefail

echo "========== FraudLens: System Prerequisites =========="

if [[ ! -f /etc/os-release ]]; then
    echo "ERROR: Cannot identify the operating system."
    exit 1
fi

. /etc/os-release

if [[ "${ID:-}" != "ubuntu" ]]; then
    echo "ERROR: This installer currently supports Ubuntu only."
    exit 1
fi

echo "Operating system: $PRETTY_NAME"

if ! command -v apt-get >/dev/null 2>&1; then
    echo "ERROR: apt-get is unavailable."
    exit 1
fi

echo
echo "Installing basic system dependencies..."

sudo apt-get update
sudo apt-get install -y \
    bash \
    curl \
    wget \
    tar \
    gzip \
    unzip \
    zip \
    procps \
    openssh-client \
    openssh-server \
    rsync \
    python3 \
    python3-pip \
    python3-numpy \
    python3-packaging

echo
echo "Checking Java 8..."

JAVA8_FOUND=false

for candidate in \
    /usr/lib/jvm/java-8-openjdk-amd64 \
    /usr/lib/jvm/temurin-8-jdk-amd64 \
    /usr/lib/jvm/temurin-8-jdk
do
    if [[ -x "$candidate/bin/java" ]]; then
        JAVA_VERSION=$("$candidate/bin/java" -version 2>&1 || true)
        if grep -q 'version "1\.8\.' <<< "$JAVA_VERSION"; then
            echo "Java 8 found: $candidate"
            JAVA8_FOUND=true
            break
        fi
    fi
done

if [[ "$JAVA8_FOUND" != true ]]; then
    echo "WARNING: Java 8 was not found in the usual installation paths."
    echo "The big-data toolchain setup must install or locate a compatible JDK."
else
    echo "Java 8 check passed."
fi

echo
echo "System prerequisites installed."
echo "Next: run setup/configure_environment.sh after the toolchain setup is complete."
