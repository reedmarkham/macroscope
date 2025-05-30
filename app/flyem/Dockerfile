###############################################################################
# Stage 1 – build the Conda environment
###############################################################################
FROM mambaorg/micromamba:1.5.7 AS builder
ARG  MAMBA_DOCKERFILE_ACTIVATE=1

ENV  MAMBA_ROOT_PREFIX=/opt/conda \
     CONDA_PKGS_DIRS=/opt/conda/pkgs

COPY environment.yml /tmp/environment.yml

USER root

RUN --mount=type=cache,id=conda-pkgs,target=/opt/conda/pkgs,sharing=locked \
    mkdir -p /opt/conda/pkgs && \
    micromamba create -y -n dvid-cropper -f /tmp/environment.yml && \
    micromamba clean  --all --yes && \
    DRACO=$(find /opt/conda/envs -maxdepth 3 -name 'libdraco.so*' -print -quit 2>/dev/null || true) && \
    if [ -n "$DRACO" ]; then \
        ln -sf "$DRACO" "$(dirname "$DRACO")/libdraco.so.1"; \
    fi

###############################################################################
# Stage 2 – slim runtime image
###############################################################################
FROM mambaorg/micromamba:1.5.7
ARG  MAMBA_DOCKERFILE_ACTIVATE=1

ENV  MAMBA_ROOT_PREFIX=/opt/conda \
     PATH=/opt/conda/envs/dvid-cropper/bin:/opt/conda/bin:$PATH

USER root
COPY --from=builder /opt/conda /opt/conda

COPY --chmod=755 docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
WORKDIR /opt/code
COPY . .

USER mambauser
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["python", "main.py"]
