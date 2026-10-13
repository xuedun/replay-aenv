FROM python:3.11.2-slim

ARG DEBIAN_FRONTEND=noninteractive
ARG DEBIAN_MIRROR=http://mirrors.aliyun.com/debian
ARG DEBIAN_SECURITY_MIRROR=http://mirrors.aliyun.com/debian-security

ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG http_proxy
ARG https_proxy
ARG NO_PROXY
ARG FTP_PROXY
ARG ftp_proxy

ENV HTTP_PROXY=${HTTP_PROXY} \
    HTTPS_PROXY=${HTTPS_PROXY} \
    http_proxy=${http_proxy} \
    https_proxy=${https_proxy} \
    NO_PROXY=${NO_PROXY} \
    FTP_PROXY=${FTP_PROXY} \
    ftp_proxy=${ftp_proxy} \
    DEBIAN_MIRROR=${DEBIAN_MIRROR} \
    DEBIAN_SECURITY_MIRROR=${DEBIAN_SECURITY_MIRROR}

RUN sed -i \
        -e "s|http://deb.debian.org/debian-security|${DEBIAN_SECURITY_MIRROR}|g" \
        -e "s|http://deb.debian.org/debian|${DEBIAN_MIRROR}|g" \
        /etc/apt/sources.list \
    && printf 'Acquire::Retries "3";\nAcquire::http::Timeout "20";\n' >/etc/apt/apt.conf.d/80-ab27-network \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        fonts-liberation \
        libasound2 \
        libatk-bridge2.0-0 \
        libatk1.0-0 \
        libatspi2.0-0 \
        libcairo2 \
        libcups2 \
        libdrm2 \
        libgbm1 \
        libglib2.0-0 \
        libgtk-3-0 \
        libnspr4 \
        libnss3 \
        libpango-1.0-0 \
        libx11-6 \
        libxcb1 \
        libxcomposite1 \
        libxdamage1 \
        libxext6 \
        libxfixes3 \
        libxkbcommon0 \
        libxrandr2 \
        libxshmfence1 \
        poppler-utils \
        procps \
        tini \
        xz-utils \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
        pypdf==6.10.0 \
        pdf2image==1.17.0 \
        Pillow==12.2.0

COPY cases/05_pdf/image_build/fixtures/scripts/ /opt/pdf/scripts/
COPY cases/05_pdf/image_build/fixtures/generate_field_values.py /opt/pdf/generate_field_values.py
COPY cases/05_pdf/image_build/fixtures/batch_driver.sh /opt/pdf/batch_driver.sh
COPY cases/05_pdf/verify_pdf_batch.py /opt/pdf/verify_pdf_batch.py
COPY cases/05_pdf/synthetic_applicants.json /opt/pdf/synthetic_applicants.json
COPY cases/05_pdf/pdf_dataset_sources.md /opt/pdf/pdf_dataset_sources.md

RUN chmod +x /opt/pdf/scripts/*.py \
    && chmod +x /opt/pdf/batch_driver.sh \
    && chmod +x /opt/pdf/verify_pdf_batch.py \
    && python3 -c "import pypdf; print('pypdf:', pypdf.__version__)" \
    && python3 -c "from pdf2image import convert_from_path; from PIL import Image; print('pdf2image + PIL: ok')" \
    && pdftoppm -v

RUN mkdir -p /root/.openclaw/workspace/tool-modeling/SUB-MEM-PDF-01/input \
             /root/.openclaw/workspace/tool-modeling/SUB-MEM-PDF-01/output \
             /root/.openclaw/skills/pdf/scripts \
    && chmod 755 /root/.openclaw/workspace \
    && ln -s /opt/pdf/scripts /root/.openclaw/skills/pdf/scripts \
    && ln -s /opt/pdf/generate_field_values.py /root/.openclaw/workspace/tool-modeling/SUB-MEM-PDF-01/generate_field_values.py \
    && ln -s /opt/pdf/batch_driver.sh /root/.openclaw/workspace/tool-modeling/SUB-MEM-PDF-01/batch_driver.sh \
    && ln -s /opt/pdf/verify_pdf_batch.py /root/.openclaw/workspace/tool-modeling/SUB-MEM-PDF-01/input/verify_pdf_batch.py \
    && ln -s /opt/pdf/synthetic_applicants.json /root/.openclaw/workspace/tool-modeling/SUB-MEM-PDF-01/input/synthetic_applicants.json \
    && ln -s /opt/pdf/pdf_dataset_sources.md /root/.openclaw/workspace/tool-modeling/SUB-MEM-PDF-01/input/pdf_dataset_sources.md

COPY cases/05_pdf/image_build/fixtures/declaration-for-federal-employment-optional-form-august-2023.pdf /root/.openclaw/workspace/tool-modeling/SUB-MEM-PDF-01/input/of306_aug2023.pdf

RUN python3 -c "from pypdf import PdfReader; r=PdfReader('/root/.openclaw/workspace/tool-modeling/SUB-MEM-PDF-01/input/of306_aug2023.pdf'); fields=r.get_fields() or {}; leaf=sum(not f.get('/Kids') and f.get('/FT') in {'/Tx','/Sig','/Ch'} for f in fields.values()); btns=sum(bool(f.get('/Kids')) and f.get('/FT')=='/Btn' for f in fields.values()); assert len(r.pages)==3 and (leaf+btns)==38, f'OF-306 validation failed: pages={len(r.pages)}, terminal_fields={leaf+btns}'" \
    && echo "OF-306 PDF validation passed"

ENV PATH=/opt/pdf:${PATH}

RUN printf 'unset HTTP_PROXY\nunset HTTPS_PROXY\nunset http_proxy\nunset https_proxy\nunset NO_PROXY\nunset FTP_PROXY\nunset ftp_proxy\n' > /etc/profile.d/99-unset-proxy.sh \
    && chmod +x /etc/profile.d/99-unset-proxy.sh

ENV HTTP_PROXY="" \
    HTTPS_PROXY="" \
    http_proxy="" \
    https_proxy="" \
    FTP_PROXY="" \
    ftp_proxy=""

WORKDIR /root/.openclaw/workspace/tool-modeling/SUB-MEM-PDF-01

CMD ["sleep", "infinity"]
