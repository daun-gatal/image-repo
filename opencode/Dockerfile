ARG VERSION

# ============================================================
# Stage 1: Builder
#
# Installs OpenCode + all requested plugins.
# If a plugin is unavailable/incompatible, the Docker build
# fails here rather than at container startup.
# ============================================================
FROM ghcr.io/anomalyco/opencode:${VERSION} AS plugin-builder

USER root

RUN apk add --no-cache \
    git \
    git-lfs \
    openssh-client \
    bash \
    curl \
    wget \
    jq \
    ca-certificates \
    github-cli \
    docker-cli

# OpenCode runtime directories.
RUN mkdir -p \
      /home/opencode/.config/opencode \
      /home/opencode/.cache/opencode \
      /home/opencode/.local/share \
      /home/opencode/.ssh \
    && chown -R 1000:1000 /home/opencode

USER 1000

ENV HOME=/home/opencode
ENV XDG_CONFIG_HOME=/home/opencode/.config
ENV XDG_CACHE_HOME=/home/opencode/.cache

# ------------------------------------------------------------
# Install all requested OpenCode plugins globally.
#
# We intentionally do NOT add @version here yet.
# This lets OpenCode resolve the latest published package,
# while the build itself validates whether it works with the
# OpenCode version in this image.
# ------------------------------------------------------------

RUN opencode plugin opencode-vibeguard --global

RUN opencode plugin @tarquinen/opencode-dcp --global

RUN opencode plugin @morphllm/opencode-morph-plugin --global

RUN opencode plugin opencode-pty --global

RUN opencode plugin @zenobius/opencode-skillful --global

RUN opencode plugin opencode-supermemory --global

RUN opencode plugin opencode-worktree --global

RUN opencode plugin opencode-conductor --global

RUN opencode plugin opencode-scheduler --global

RUN opencode plugin opencode-notify --global

RUN opencode plugin @franlol/opencode-md-table-formatter --global

RUN opencode plugin @openspoon/subtask2 --global

RUN opencode plugin opencode-goal-plugin --global

RUN opencode plugin opencode-websearch-cited --global

RUN opencode plugin @plannotator/opencode --global

RUN opencode plugin opencode-wakatime --global

RUN opencode plugin opencode-zellij-namer --global

RUN opencode plugin @nick-vi/opencode-type-inject --global

# ------------------------------------------------------------
# Show the resulting configuration during build.
# Useful for CI logs and debugging.
# ------------------------------------------------------------
RUN echo "===== OpenCode config =====" \
    && cat /home/opencode/.config/opencode/opencode.json 2>/dev/null \
       || cat /home/opencode/.config/opencode/opencode.jsonc 2>/dev/null \
    && echo "===== Plugin cache =====" \
    && find /home/opencode/.cache/opencode -maxdepth 2 -type d 2>/dev/null | sort


# ============================================================
# Stage 2: Runtime
#
# Starts from a clean OpenCode image and copies only the
# generated plugin/config/cache state from the builder.
# ============================================================
FROM ghcr.io/anomalyco/opencode:${VERSION}

USER root

RUN apk add --no-cache \
    git \
    git-lfs \
    openssh-client \
    bash \
    curl \
    wget \
    jq \
    ca-certificates \
    github-cli \
    docker-cli

# Git HTTPS authentication helper.
RUN cat > /usr/local/bin/git-askpass <<'EOF'
#!/bin/sh
case "$1" in
  *Username*)
    echo "x-access-token"
    ;;
  *Password*)
    echo "$GITHUB_TOKEN"
    ;;
  *)
    echo ""
    ;;
esac
EOF

RUN chmod 755 /usr/local/bin/git-askpass

# OpenCode runtime directories.
RUN mkdir -p \
      /home/opencode \
      /home/opencode/.config \
      /home/opencode/.cache \
      /home/opencode/.local/share \
      /home/opencode/.ssh \
    && chown -R 1000:1000 /home/opencode

ENV HOME=/home/opencode
ENV XDG_CONFIG_HOME=/home/opencode/.config
ENV XDG_CACHE_HOME=/home/opencode/.cache

# ------------------------------------------------------------
# Copy globally installed OpenCode plugins/config from builder.
# ------------------------------------------------------------

COPY --from=plugin-builder \
    --chown=1000:1000 \
    /home/opencode/.config/opencode \
    /home/opencode/.config/opencode

COPY --from=plugin-builder \
    --chown=1000:1000 \
    /home/opencode/.cache/opencode \
    /home/opencode/.cache/opencode

COPY --from=plugin-builder \
    --chown=1000:1000 \
    /home/opencode/.local/share \
    /home/opencode/.local/share

USER 1000

ENTRYPOINT ["opencode"]
