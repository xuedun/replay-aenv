# Base image - Ubuntu 24.04 ARM64
FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/ubuntu:24.04-linuxarm64

# Set environment variables to avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Configure proxy (empty by default, set via --build-arg if needed)
ARG HTTP_PROXY=
ARG HTTPS_PROXY=
ENV http_proxy=${HTTP_PROXY} \
    https_proxy=${HTTPS_PROXY} \
    HTTP_PROXY=${HTTP_PROXY} \
    HTTPS_PROXY=${HTTPS_PROXY}

# 1. 仅安装基础工具 (移除了手动指定的底层图形库，避免 Ubuntu 24.04 的 t64 包名变更导致报错)
RUN apt update && apt install -y --no-install-recommends \
    wget xz-utils vim iproute2 net-tools gnupg git \
    ca-certificates curl fonts-liberation procps \
    && rm -rf /var/lib/apt/lists/*

# 2. 安装 Node.js v24
ARG NODE_VERSION=24.14.0
RUN cd /usr/local && \
    wget --no-check-certificate https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-arm64.tar.xz && \
    tar -xvf node-v${NODE_VERSION}-linux-arm64.tar.xz && \
    mv node-v${NODE_VERSION}-linux-arm64 node && \
    rm -rf node-v${NODE_VERSION}-linux-arm64.tar.xz && \
    ln -sf /usr/local/node/bin/node /usr/local/bin/node && \
    ln -sf /usr/local/node/bin/npm /usr/local/bin/npm && \
    ln -sf /usr/local/node/bin/npx /usr/local/bin/npx && \
    git config --global http.sslVerify false

# 3. 添加 xtradeb PPA 并安装 Chromium (apt 会自动解析并安装所有正确的 t64 依赖库)
RUN mkdir -p /usr/share/keyrings && \
    wget --no-check-certificate -O /tmp/xtradeb.asc "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x82BB6851C64F6880" && \
    gpg --dearmor -o /usr/share/keyrings/xtradeb-keyring.gpg /tmp/xtradeb.asc && \
    rm /tmp/xtradeb.asc

RUN echo 'Types: deb\nURIs: https://ppa.launchpadcontent.net/xtradeb/apps/ubuntu/\nSuites: noble\nComponents: main\nSigned-By: /usr/share/keyrings/xtradeb-keyring.gpg' > /etc/apt/sources.list.d/xtradeb-apps.sources

RUN apt update -o Acquire::https::Verify-Peer=false -o Acquire::https::Verify-Host=false && \
    apt install -y -o Acquire::https::Verify-Peer=false -o Acquire::https::Verify-Host=false chromium && \
    apt clean && \
    rm -rf /var/lib/apt/lists/* /var/tmp/* /tmp/*

# 4. 安装 agent-browser (强制跳过内置 Chromium 下载)
RUN npm config set strict-ssl false && \
    npm config set registry https://registry.npmmirror.com && \
    PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1 PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=1 npm install -g agent-browser

# 5. 创建 replay.json 所需的工作空间目录
RUN mkdir -p /root/.openclaw/workspace \
             /root/.openclaw/workspace/tool-modeling/SUB-ALL-BROWSER-01/output \
             /tmp/ab27-logs \
    && chmod 755 /root/.openclaw/workspace

# 6. 核心粘合配置：指向系统 Chromium，并禁用 GPU 防止 ARM64 崩溃
ENV AGENT_BROWSER_EXECUTABLE_PATH=/usr/bin/chromium \
    AGENT_BROWSER_ARGS=--no-sandbox,--disable-crash-reporter,--disable-dev-shm-usage,--disable-gpu,--window-size=1920,1080,--start-maximized \
    PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH=/usr/bin/chromium \
    PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium \
    PATH=/usr/local/node/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# 清理代理环境变量
ENV http_proxy= \
    https_proxy= \
    HTTP_PROXY= \
    HTTPS_PROXY=

WORKDIR /root/.openclaw/workspace

CMD ["sleep", "infinity"]