ARG VERSION

# ============================================================
# Stage 1: Build OpenCode + install plugins
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

RUN mkdir -p \
      /home/opencode/.config/opencode \
      /home/opencode/.cache/opencode \
      /home/opencode/.local/share \
      /home/opencode/.ssh \
    && chown -R opencode:opencode /home/opencode

# ============================================================
# OpenCode
# ============================================================
RUN curl -fsSL https://opencode.ai/install \
    | bash -s -- --version "${VERSION}" \
    && if [ -x /root/.opencode/bin/opencode ]; then \
         install -m 0755 /root/.opencode/bin/opencode /usr/local/bin/opencode; \
       elif [ -x /root/.local/bin/opencode ]; then \
         install -m 0755 /root/.local/bin/opencode /usr/local/bin/opencode; \
       else \
         echo "ERROR: OpenCode executable not found after installation"; \
         find /root -type f -name opencode -print; \
         exit 1; \
       fi

# ============================================================
# Runtime environment
# ============================================================
ENV HOME=/home/opencode
ENV XDG_CONFIG_HOME=/home/opencode/.config
ENV XDG_CACHE_HOME=/home/opencode/.cache
ENV PATH="/usr/local/bin:/usr/bin:/bin"
ENV BUN_RUNTIME_TRANSPILER_CACHE_PATH=0

USER opencode

# ============================================================
# Verify OpenCode version
# ============================================================
RUN set -eux; \
    opencode --version; \
    opencode --version | grep -F "${VERSION}"

# ============================================================
# Avoid proxy-related plugin installation issues
# ============================================================
ENV HTTP_PROXY=
ENV HTTPS_PROXY=
ENV ALL_PROXY=
ENV http_proxy=
ENV https_proxy=
ENV all_proxy=

# ============================================================
# Install all requested plugins
# ============================================================

# 1. VibeGuard
RUN opencode plugin opencode-vibeguard@0.1.0 --global

# 2. Dynamic Context Pruning
RUN opencode plugin @tarquinen/opencode-dcp@3.1.15 --global

# 3. Morph
RUN opencode plugin @morphllm/opencode-morph-plugin@2.0.16 --global

# 4. PTY
# RUN opencode plugin opencode-pty@0.3.6 --global

# 5. Skillful
RUN opencode plugin @zenobius/opencode-skillful@1.2.5 --global

# 6. Supermemory
RUN opencode plugin opencode-supermemory@2.0.12 --global

# 7. Worktree
# Replacement for: opencode-worktree
RUN opencode plugin @nevermore93/opencode-worktree@0.5.3 --global

# 8. Conductor
# Replacement for: opencode-conductor
RUN opencode plugin opencode-conductor-plugin@1.32.0 --global

# 9. Scheduler
RUN opencode plugin opencode-scheduler@1.3.0 --global

# 10. Notify
# Replacement for: opencode-notify
RUN opencode plugin @freely01/opencode-notify@0.5.2 --global

# 11. Markdown table formatter
RUN opencode plugin @franlol/opencode-md-table-formatter@0.0.6 --global

# 12. Subtask2
RUN opencode plugin @openspoon/subtask2@0.3.9 --global

# 13. Goal plugin
# Replacement for: opencode-goal-plugin
RUN opencode plugin @prevalentware/opencode-goal-plugin@0.1.39 --global

# 14. Web search with citations
RUN opencode plugin opencode-websearch-cited@1.2.0 --global

# 15. Plannotator
# Replacement for: @plannotator/opencode
RUN opencode plugin @knownasred/plannotator-opencode@0.6.7 --global

# 16. WakaTime
RUN opencode plugin opencode-wakatime@1.3.9 --global

# 17. Zellij namer
RUN opencode plugin opencode-zellij-namer@1.1.3 --global

# 18. Type inject
RUN opencode plugin @nick-vi/opencode-type-inject@1.3.1 --global

# ============================================================
# Build diagnostics
# ============================================================
RUN echo "========================================" \
    && echo "OpenCode version:" \
    && opencode --version \
    && echo "========================================" \
    && echo "OpenCode config:" \
    && cat /home/opencode/.config/opencode/opencode.jsonc \
    && echo "========================================" \
    && echo "Plugin cache size:" \
    && du -sh /home/opencode/.cache/opencode


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
    && install -m 0755 /tmp/docker/docker /usr/local/bin/docker \
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
# OpenCode config
# ============================================================
COPY --from=builder \
    --chown=opencode:opencode \
    /home/opencode/.config/opencode \
    /home/opencode/.config/opencode

# ============================================================
# Plugin cache
# ============================================================
COPY --from=builder \
    --chown=opencode:opencode \
    /home/opencode/.cache/opencode \
    /home/opencode/.cache/opencode

# ============================================================
# OpenCode local data
# ============================================================
COPY --from=builder \
    --chown=opencode:opencode \
    /home/opencode/.local \
    /home/opencode/.local

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
