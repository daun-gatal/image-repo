ARG VERSION
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
# Git calls this program when authentication is required.
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

# Make sure OpenCode's home exists and belongs to UID 1000.
RUN mkdir -p \
      /home/opencode \
      /home/opencode/.config \
      /home/opencode/.cache \
      /home/opencode/.local/share \
      /home/opencode/.ssh \
    && chown -R 1000:1000 /home/opencode

USER 1000

ENTRYPOINT ["opencode"]
