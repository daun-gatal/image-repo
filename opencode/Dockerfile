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
      "https://download.docker.com/linux/static/stable/x86_64/docker-27.5.1.tgz" \
      -o /tmp/docker.tgz \
    && tar -xzf /tmp/docker.tgz -C /tmp \
    && install -m 0755 \
        /tmp/docker/docker \
        /usr/local/bin/docker \
    && rm -rf /tmp/docker /tmp/docker.tgz

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
# OpenCode directories
# ============================================================
RUN mkdir -p \
      /home/opencode/.config \
      /home/opencode/.cache \
      /home/opencode/.local \
      /home/opencode/.ssh \
    && chown -R opencode:opencode /home/opencode

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
ENV PATH="/usr/local/bin:/usr/bin:/bin"
ENV BUN_RUNTIME_TRANSPILER_CACHE_PATH=0

# ============================================================
# Verify installation
# ============================================================
RUN set -eux; \
    git --version; \
    git-lfs --version; \
    gh --version; \
    docker --version; \
    opencode --version

# ============================================================
# Runtime
# ============================================================
USER opencode

WORKDIR /workspace

ENTRYPOINT ["opencode"]
