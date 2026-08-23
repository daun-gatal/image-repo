ARG VERSION

# ============================================================
# Stage 1: Plugin builder
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

# Prevent proxy variables from interfering with plugin
# installation during the image build.
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
# Original: opencode-worktree
RUN opencode plugin @nevermore93/opencode-worktree@0.5.3 --global

# 8. Conductor
# Original: opencode-conductor
RUN opencode plugin opencode-conductor-plugin@1.32.0 --global

# 9. Scheduler
RUN opencode plugin opencode-scheduler@1.3.0 --global

# 10. Notify
# Original: opencode-notify
RUN opencode plugin @freely01/opencode-notify@0.5.2 --global

# 11. Markdown table formatter
RUN opencode plugin @franlol/opencode-md-table-formatter@0.0.6 --global

# 12. Subtask2
RUN opencode plugin @openspoon/subtask2@0.3.9 --global

# 13. Goal plugin
# Original: opencode-goal-plugin
RUN opencode plugin @prevalentware/opencode-goal-plugin@0.1.39 --global

# 14. Web search with citations
# Keep this LAST.
RUN opencode plugin opencode-websearch-cited@1.2.0 --global

# 15. Plannotator
# Original: @plannotator/opencode
RUN opencode plugin @knownasred/plannotator-opencode@0.6.7 --global

# 16. WakaTime
RUN opencode plugin opencode-wakatime@1.3.9 --global

# 17. Zellij namer
RUN opencode plugin opencode-zellij-namer@1.1.3 --global

# 18. Type inject
RUN opencode plugin @nick-vi/opencode-type-inject@1.3.1 --global

# ============================================================
# Build-time diagnostics
# ============================================================

RUN echo "==================================================" \
    && echo "OpenCode version" \
    && opencode --version \
    && echo "==================================================" \
    && echo "Generated config" \
    && cat /home/opencode/.config/opencode/opencode.jsonc \
    && echo "==================================================" \
    && echo "Installed plugin cache" \
    && find /home/opencode/.cache/opencode -maxdepth 3 -type d | sort


# ============================================================
# Stage 2: Runtime
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

# ============================================================
# Git HTTPS authentication helper
# ============================================================

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

# ============================================================
# OpenCode directories
# ============================================================

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

# ============================================================
# Copy plugin configuration/cache from builder
# ============================================================

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
