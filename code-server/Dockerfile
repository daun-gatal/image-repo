ARG VERSION
FROM codercom/code-server:${VERSION}

USER root

# ============================================================
# System dependencies
#
# Includes:
#   - git
#   - git-lfs
#   - SSH client
#   - bash
#   - curl
#   - wget
#   - jq
#   - CA certificates
#   - gnupg
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
#
# The Docker daemon is NOT installed here.
#
# Runtime:
#   DOCKER_HOST=tcp://opencode-dind:2375
# ============================================================
RUN curl -fsSL \
      https://download.docker.com/linux/static/stable/x86_64/docker-27.5.1.tgz \
      -o /tmp/docker.tgz \
    && tar -xzf /tmp/docker.tgz -C /tmp \
    && install -m 0755 /tmp/docker/docker /usr/local/bin/docker \
    && rm -rf /tmp/docker /tmp/docker.tgz

# ============================================================
# OpenCode
# ============================================================
RUN curl -fsSL https://opencode.ai/install \
      | bash

# ============================================================
# Make OpenCode globally available
#
# The installer may place OpenCode under:
#   /root/.opencode/bin/opencode
#
# Put it in a standard PATH location.
# ============================================================
RUN if [ -x /root/.opencode/bin/opencode ]; then \
      ln -sf /root/.opencode/bin/opencode /usr/local/bin/opencode; \
    elif [ -x /root/.local/bin/opencode ]; then \
      ln -sf /root/.local/bin/opencode /usr/local/bin/opencode; \
    else \
      echo "ERROR: OpenCode executable not found after installation"; \
      find /root -type f -name opencode -print; \
      exit 1; \
    fi

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
# Code-server user directories
# ============================================================
RUN mkdir -p \
      /home/coder/.config \
      /home/coder/.cache \
      /home/coder/.local \
      /home/coder/.ssh \
    && chown -R coder:coder /home/coder

# ============================================================
# Verify everything during build
# ============================================================
RUN set -eux; \
    git --version; \
    git-lfs --version; \
    gh --version; \
    docker --version; \
    opencode --version

# ============================================================
# Runtime user
# ============================================================
USER coder

WORKDIR /workspace

ENTRYPOINT ["code-server"]