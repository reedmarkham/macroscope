###############################################################################
# Stage 1: “builder” — build libdvid-cpp & neuclease using flyem/flyem-build
###############################################################################
FROM flyem/flyem-build:latest AS builder

WORKDIR /opt/builder

# 1) Clone the flyem-conda-recipes repo (contains all FlyEM conda recipes)
RUN git clone --depth=1 https://github.com/janelia-flyem/flyem-conda-recipes.git

# 2) Install a PyYAML version compatible with publish-conda-stack, then run publish-conda-stack
RUN conda run -n base /bin/sh -lc "\
      conda install -n base -c conda-forge pyyaml=5.4.1 -y && \
      cd flyem-conda-recipes && \
      publish-conda-stack flyem-recipe-specs.yaml libdvid-cpp neuclease \
    "

# 3) Copy all built tarballs (*.tar.bz2) into /tmp/packages
RUN mkdir -p /tmp/packages && \
    cp flyem-conda-recipes/conda-bld/linux-64/*.tar.bz2 /tmp/packages/

###############################################################################
# Stage 2: “runtime” — install those packages into a clean micromamba env
###############################################################################
FROM mambaorg/micromamba:1.5.6

# 1) Set environment variables for Micromamba
ENV MAMBA_DOCKERFILE_ACTIVATE=1 \
    CONDA_ENV_NAME=dvid-cropper \
    PATH=/opt/conda/envs/${CONDA_ENV_NAME}/bin:$PATH

# 2) Copy built tarballs from Stage 1 into /opt/packages (this becomes a local channel)
COPY --from=builder /tmp/packages /opt/packages

# 3) Create a fresh environment using our local channel first, then conda-forge
RUN micromamba create -y -n ${CONDA_ENV_NAME} \
      -c file:///opt/packages \
      -c conda-forge \
      python=3.12 \
      neuclease \
      libdvid-cpp \
    && micromamba clean --all --yes

# 4) (Optional) Sanity check: verify that libdvid & neuclease import correctly
RUN micromamba run -n ${CONDA_ENV_NAME} python - <<'PYCODE'
import neuclease, libdvid, numpy, pandas
print("✅  neuclease", neuclease.__version__,
      "|  libdvid OK",
      "|  numpy", numpy.__version__,
      "|  pandas", pandas.__version__)
PYCODE

# 5) Copy your application code
WORKDIR /opt/code
COPY . .

# 6) Entrypoint: run "python main.py" inside dvid-cropper env
ENTRYPOINT ["micromamba", "run", "-n", "dvid-cropper"]
CMD ["python", "main.py"]
