ARG VERSION
FROM codercom/code-server:${VERSION}

USER root

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# ============================================================
# System dependencies
# ============================================================
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        git-lfs \
        openssh-client \
        bash \
        curl \
        wget \
        jq \
        ca-certificates \
        gnupg \
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
ARG DOCKER_VERSION=27.5.1
ARG COMPOSE_VERSION=5.5.1
ARG OPENCODE_VERSION=2.0.14

RUN curl -fsSL \
      "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_VERSION}.tgz" \
      -o /tmp/docker.tgz \
    && tar -xzf /tmp/docker.tgz -C /tmp \
    && install -m 0755 /tmp/docker/docker /usr/local/bin/docker \
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
# OpenCode (V2, installed as opencode2; keep the V1 command
# name working via symlink)
# ============================================================
RUN export OPENCODE_INSTALL_DIR=/usr/local/bin \
    && curl -fsSL https://opencode.ai/v2/install | bash -s -- --version "${OPENCODE_VERSION}" \
    && if [[ -x /usr/local/bin/opencode2 ]]; then \
         chmod 755 /usr/local/bin/opencode2; \
       else \
         echo "ERROR: OpenCode executable not found after installation"; \
         find /usr/local/bin /root -type f -name 'opencode*' -print; \
         exit 1; \
       fi \
    && ln -s opencode2 /usr/local/bin/opencode

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
# Code-server user directories
# ============================================================
RUN mkdir -p \
      /home/coder/.config \
      /home/coder/.cache \
      /home/coder/.local \
      /home/coder/.ssh \
    && chown -R coder:coder /home/coder

# ============================================================
# Verify installation
# ============================================================
RUN set -eux; \
    git --version; \
    git-lfs --version; \
    gh --version; \
    docker --version; \
    docker compose version; \
    opencode --version; \
    opencode2 --version

# ============================================================
# Runtime
# ============================================================
USER coder

ENV PATH="/usr/local/bin:/usr/bin:/bin"

WORKDIR /workspace

ENTRYPOINT ["code-server"]
