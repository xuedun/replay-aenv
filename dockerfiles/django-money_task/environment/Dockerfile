ARG BASE_IMAGE=harbor-swe-tools:base
FROM ${BASE_IMAGE}

ARG TARGET_REPO=https://gh-proxy.com/https://github.com/django-money/django-money.git
ARG TARGET_COMMIT=835c1ab8
ARG PIP_INDEX_URL=https://mirrors.aliyun.com/pypi/simple
ARG PIP_DEFAULT_TIMEOUT=300
ARG DJANGO_VERSION=5.1.6
ARG PYTEST_VERSION=8.2.2
ARG PYTEST_DJANGO_VERSION=4.10.0
ARG PYTEST_COV_VERSION=6.0.0
ARG TZDATA_VERSION=2026.3

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

USER root
WORKDIR /testbed

# Snapshot payload generator used by replay trajectories.  The tool writes
# one exact 1 MiB high-entropy file for every requested unit, so filesystem
# compression cannot collapse the payload like zero-filled files would.
COPY --chmod=0755 environment/writeTxt /usr/local/bin/writeTxt

# 拉取当前 issue 对应的 django-money 仓库和基准 commit。
RUN set -eux; \
    rm -rf /testbed/* /testbed/.[!.]* /testbed/..?* 2>/dev/null || true; \
    git clone "${TARGET_REPO}" /testbed; \
    git checkout "${TARGET_COMMIT}"; \
    git status --short; \
    git rev-parse HEAD; \
    test "$(git rev-parse --short=8 HEAD)" = "${TARGET_COMMIT}"

# 安装 django-money 本体及其测试依赖，并锁定原始轨迹使用的关键版本。
#
# 该 commit 的 setup.py 提供：
#   test     -> pytest、pytest-django、pytest-cov、mixer、mypy、django-stubs
#   exchange -> certifi
#
# 使用 --no-build-isolation，避免 editable 构建时看不到当前环境里的
# setuptools/wheel 等依赖。
RUN set -eux; \
    python3 -m pip install --no-cache-dir \
        "pip<25" \
        "setuptools<75" \
        wheel; \
    cd /testbed; \
    python3 -m pip install \
        --no-cache-dir \
        --no-build-isolation \
        -e '.[test,exchange]' \
        "Django==${DJANGO_VERSION}" \
        "pytest==${PYTEST_VERSION}" \
        "pytest-django==${PYTEST_DJANGO_VERSION}" \
        "pytest-cov==${PYTEST_COV_VERSION}" \
        "tzdata==${TZDATA_VERSION}"; \
    command -v python >/dev/null 2>&1 \
        || ln -s "$(command -v python3)" /usr/local/bin/python

# 原始轨迹中的少量只读检查写死了 Conda site-packages 路径；当前镜像
# 使用 Ubuntu 系统 Python，因此将原路径兼容映射到容器内动态发现的
# purelib 目录。该映射不引用宿主机路径，可跨服务器和 CPU 架构构建。
RUN set -eux; \
    python_site_dir="$(python3 -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')"; \
    test -d "${python_site_dir}"; \
    test "$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" = "3.10"; \
    mkdir -p /opt/miniconda3/envs/testbed/lib/python3.10; \
    ln -sfn \
        "${python_site_dir}" \
        /opt/miniconda3/envs/testbed/lib/python3.10/site-packages; \
    test -f /opt/miniconda3/envs/testbed/lib/python3.10/site-packages/django/forms/fields.py

# 构建期验证：
# - 确认源码从 /testbed 加载
# - 确认测试依赖存在
# - 确认 replay 所需编辑工具存在
RUN set -eux; \
    swe-tool-shell bash -lc '\
        cd /testbed; \
        python3 -c "import django; print(\"Django:\", django.get_version())"; \
        python3 -c "from importlib.metadata import version; assert version(\"Django\") == \"${DJANGO_VERSION}\"; assert version(\"pytest\") == \"${PYTEST_VERSION}\"; assert version(\"pytest-django\") == \"${PYTEST_DJANGO_VERSION}\"; assert version(\"pytest-cov\") == \"${PYTEST_COV_VERSION}\"; assert version(\"tzdata\") == \"${TZDATA_VERSION}\""; \
        python3 -c "from zoneinfo import ZoneInfo; print(\"timezone:\", ZoneInfo(\"America/Chicago\"))"; \
        python3 -c "import djmoney; print(\"django-money:\", djmoney.__version__, djmoney.__file__)"; \
        python3 -c "import moneyed; print(\"py-moneyed:\", moneyed.__file__)"; \
        python3 -c "import pytest; print(\"pytest:\", pytest.__version__)"; \
        python3 -c "import pytest_django; print(\"pytest-django:\", pytest_django.__file__)"; \
        python3 -c "import registry; print(\"registry:\", registry)"; \
        command -v str_replace_editor; \
        command -v writeTxt; \
        writeTxt 0; \
        command -v python; \
        command -v pytest; \
        git rev-parse HEAD \
    '

# 可选但推荐：提前做测试收集，尽早发现依赖或 Django 配置问题。
RUN set -eux; \
    swe-tool-shell bash -lc '\
        cd /testbed; \
        pytest --collect-only -q \
    '

CMD ["bash"]
