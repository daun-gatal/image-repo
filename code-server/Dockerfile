ARG VERSION
FROM codercom/code-server:${VERSION}

USER root

# ============================================================
# Base tools
# ============================================================
RUN apt-get update && apt-get install -y \
    git \
    git-lfs \
    openssh-client \
    bash \
    curl \
    wget \
    jq \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ============================================================
# GitHub CLI
# ============================================================
RUN type -p curl >/dev/null \
    && curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
        -o /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
        > /etc/apt/sources.list.d/github-cli.list \
    && apt-get update \
    && apt-get install -y gh \
    && rm -rf /var/lib/apt/lists/*

# ============================================================
# Docker CLI
#
# Only the Docker CLI is installed here.
# The Docker daemon lives in the separate DinD deployment.
# ============================================================
ARG DOCKER_VERSION=27.5.1

RUN curl -fsSL \
        "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_VERSION}.tgz" \
        -o /tmp/docker.tgz \
    && tar -xzf /tmp/docker.tgz \
        --strip-components=1 \
        -C /usr/local/bin \
        docker/docker \
    && rm -f /tmp/docker.tgz \
    && chmod 755 /usr/local/bin/docker

# ============================================================
# OpenCode
#
# Install OpenCode system-wide so the `coder` user can find it.
# ============================================================
RUN curl -fsSL https://opencode.ai/install \
        -o /tmp/install-opencode.sh \
    && chmod +x /tmp/install-opencode.sh \
    && /tmp/install-opencode.sh \
    && OPENCODE_BIN="$(find /root /tmp -type f -name opencode -executable 2>/dev/null | head -n 1)" \
    && test -n "$OPENCODE_BIN" \
    && cp "$OPENCODE_BIN" /usr/local/bin/opencode \
    && chmod 755 /usr/local/bin/opencode \
    && rm -f /tmp/install-opencode.sh

# ============================================================
# Make system-wide binaries available
# ============================================================
ENV PATH="/usr/local/bin:${PATH}"

# ============================================================
# Verify everything while building the image
# ============================================================
RUN git --version \
    && git-lfs --version \
    && gh --version \
    && docker --version \
    && which opencode \
    && opencode --version

# ============================================================
# Run as code-server user
# ============================================================
USER coder

WORKDIR /workspace

# ============================================================
# Verify OpenCode is also available to the non-root user
# ============================================================
RUN which opencode \
    && opencode --version \
    && which docker \
    && docker --version \
    && which gh \
    && git --version

ENTRYPOINT ["code-server"]