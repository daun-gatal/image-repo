ARG VERSION

# ============================================================
# Stage 1: Build OpenCode
# ============================================================
FROM debian:bookworm-slim AS builder

ARG VERSION

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# ============================================================
# System dependencies
# ============================================================
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        git \
        git-lfs \
        jq \
        openssh-client \
        ripgrep \
        wget \
    && rm -rf /var/lib/apt/lists/*

# ============================================================
# OpenCode
# ============================================================
RUN echo "Installing OpenCode ${VERSION}" \
    && curl -fsSL https://opencode.ai/install \
        | bash -s -- --version "${VERSION}" \
    && if [ -x /root/.opencode/bin/opencode ]; then \
         install -m 0755 \
             /root/.opencode/bin/opencode \
             /usr/local/bin/opencode; \
       elif [ -x /root/.local/bin/opencode ]; then \
         install -m 0755 \
             /root/.local/bin/opencode \
             /usr/local/bin/opencode; \
       else \
         echo "ERROR: OpenCode executable not found after installation"; \
         find /root -type f -name opencode -print; \
         exit 1; \
       fi

# ============================================================
# Verify OpenCode
# ============================================================
RUN set -eux; \
    opencode --version; \
    test "$(opencode --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)" = "${VERSION}"


# ============================================================
# Stage 2: Runtime
# ============================================================
FROM debian:bookworm-slim

ARG DOCKER_VERSION=27.5.1
ARG COMPOSE_VERSION=5.5.1
ARG NODE_VERSION=22.23.2
ARG UV_VERSION=0.12.7
ARG GO_VERSION=1.27.0
ARG JAVA_VERSION=21
ARG MAVEN_VERSION=3.9.16

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# ============================================================
# System dependencies
# ============================================================
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        file \
        git \
        git-lfs \
        jq \
        less \
        openssh-client \
        procps \
        ripgrep \
        unzip \
        wget \
        zip \
    && rm -rf /var/lib/apt/lists/*

# ============================================================
# GitHub CLI
# ============================================================
RUN curl -fsSL \
      https://cli.github.com/packages/githubcli-archive-keyring.gpg \
      -o /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
      > /etc/apt/sources.list.d/github-cli.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends gh \
    && rm -rf /var/lib/apt/lists/*

# ============================================================
# Docker CLI
# ============================================================
RUN curl -fsSL \
      "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_VERSION}.tgz" \
      -o /tmp/docker.tgz \
    && tar -xzf /tmp/docker.tgz -C /tmp \
    && install -m 0755 \
        /tmp/docker/docker \
        /usr/local/bin/docker \
    && rm -rf /tmp/docker /tmp/docker.tgz

# ============================================================
# Docker Compose (CLI plugin)
# ============================================================
RUN curl -fsSL \
      "https://github.com/docker/compose/releases/download/v${COMPOSE_VERSION}/docker-compose-linux-x86_64" \
      -o /tmp/docker-compose \
    && install -Dm 0755 /tmp/docker-compose \
        /usr/local/lib/docker/cli-plugins/docker-compose \
    && rm -f /tmp/docker-compose

# ============================================================
# Python toolchain
# ============================================================
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-venv \
        python-is-python3 \
    && rm -rf /var/lib/apt/lists/*

# ============================================================
# uv (Python package and environment manager)
# ============================================================
RUN curl -fsSL \
      "https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/uv-x86_64-unknown-linux-gnu.tar.gz" \
      -o /tmp/uv.tgz \
    && tar -xzf /tmp/uv.tgz -C /tmp \
    && install -m 0755 \
        /tmp/uv-x86_64-unknown-linux-gnu/uv \
        /tmp/uv-x86_64-unknown-linux-gnu/uvx \
        /usr/local/bin/ \
    && rm -rf /tmp/uv-x86_64-unknown-linux-gnu /tmp/uv.tgz

# ============================================================
# Node.js
# ============================================================
RUN curl -fsSL \
      "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-x64.tar.gz" \
      -o /tmp/node.tgz \
    && mkdir -p /opt/node \
    && tar -xzf /tmp/node.tgz -C /opt/node --strip-components=1 \
    && ln -s /opt/node/bin/node /usr/local/bin/node \
    && ln -s /opt/node/bin/npm /usr/local/bin/npm \
    && ln -s /opt/node/bin/npx /usr/local/bin/npx \
    && ln -s /opt/node/bin/corepack /usr/local/bin/corepack \
    && rm -rf /opt/node/share/man \
    && rm -f /tmp/node.tgz

# ============================================================
# Go
# ============================================================
RUN curl -fsSL \
      "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" \
      -o /tmp/go.tgz \
    && tar -xzf /tmp/go.tgz -C /usr/local \
    && rm -rf \
        /usr/local/go/doc \
        /usr/local/go/misc \
        /usr/local/go/test \
        /usr/local/go/api \
    && rm -f /usr/local/go/robots.txt /usr/local/go/favicon.ico /tmp/go.tgz

# ============================================================
# Java (Temurin JDK)
# ============================================================
RUN mkdir -p /opt/java \
    && curl -fsSL \
      "https://api.adoptium.net/v3/binary/latest/${JAVA_VERSION}/ga/linux/x64/jdk/hotspot/normal/eclipse" \
      -o /tmp/java.tgz \
    && tar -xzf /tmp/java.tgz -C /opt/java --strip-components=1 \
    && rm -rf \
        /opt/java/jmods \
        /opt/java/man \
        /opt/java/demo \
        /opt/java/lib/src.zip \
    && rm -f /tmp/java.tgz

# ============================================================
# Maven
# ============================================================
RUN curl -fsSL \
      "https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz" \
      -o /tmp/maven.tgz \
    && mkdir -p /opt/maven \
    && tar -xzf /tmp/maven.tgz -C /opt/maven --strip-components=1 \
    && ln -s /opt/maven/bin/mvn /usr/local/bin/mvn \
    && rm -f /tmp/maven.tgz

# ============================================================
# OpenCode user
# ============================================================
RUN groupadd --gid 1000 opencode \
    && useradd \
        --uid 1000 \
        --gid 1000 \
        --create-home \
        --home-dir /home/opencode \
        --shell /bin/bash \
        opencode

# ============================================================
# Git HTTPS authentication helper
# ============================================================
RUN printf '%s\n' \
    '#!/bin/sh' \
    '' \
    'case "$1" in' \
    '  *Username*)' \
    '    echo "x-access-token"' \
    '    ;;' \
    '  *Password*)' \
    '    echo "$GITHUB_TOKEN"' \
    '    ;;' \
    '  *)' \
    '    echo ""' \
    '    ;;' \
    'esac' \
    > /usr/local/bin/git-askpass \
    && chmod 755 /usr/local/bin/git-askpass

# ============================================================
# OpenCode binary
# ============================================================
COPY --from=builder \
    /usr/local/bin/opencode \
    /usr/local/bin/opencode

# ============================================================
# Runtime environment
# ============================================================
ENV HOME=/home/opencode
ENV XDG_CONFIG_HOME=/home/opencode/.config
ENV XDG_CACHE_HOME=/home/opencode/.cache
ENV PATH="/home/opencode/.local/bin:/home/opencode/.npm-global/bin:/usr/local/go/bin:/opt/java/bin:/usr/local/bin:/usr/bin:/bin"
ENV JAVA_HOME=/opt/java
ENV MAVEN_HOME=/opt/maven
ENV GOPATH=/home/opencode/go
ENV NPM_CONFIG_PREFIX=/home/opencode/.npm-global
ENV PIP_NO_INPUT=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_BREAK_SYSTEM_PACKAGES=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV BUN_RUNTIME_TRANSPILER_CACHE_PATH=0

# ============================================================
# Verify installation
# ============================================================
RUN set -eux; \
    git --version; \
    git-lfs --version; \
    gh --version; \
    docker --version; \
    docker compose version; \
    python3 --version; \
    pip3 --version; \
    uv --version; \
    node --version; \
    npm --version; \
    go version; \
    java -version; \
    javac -version; \
    mvn --version; \
    opencode --version

# ============================================================
# OpenCode directories
# (last step as root: reclaims ownership of files created by
# the verification run above)
# ============================================================
RUN mkdir -p \
      /home/opencode/.config \
      /home/opencode/.cache \
      /home/opencode/.local \
      /home/opencode/.local/bin \
      /home/opencode/.npm-global \
      /home/opencode/.ssh \
    && chown -R opencode:opencode /home/opencode

# ============================================================
# Runtime
# ============================================================
USER opencode

WORKDIR /workspace

ENTRYPOINT ["opencode"]
