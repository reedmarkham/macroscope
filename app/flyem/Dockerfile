FROM continuumio/miniconda3:4.12.0

ENV CONDA_ENV_NAME=dvid-cropper \
    PATH=/opt/conda/envs/dvid-cropper/bin:$PATH

# Install conda-build in base environment
RUN conda install -y -n base conda-build

# Copy environment definition and create the environment
COPY environment.yml /tmp/environment.yml

RUN conda env create -n $CONDA_ENV_NAME -f /tmp/environment.yml && \
    conda clean --all --yes && \
    DRACO=$(find /opt/conda/envs -maxdepth 3 -name 'libdraco.so*' -print -quit 2>/dev/null || true) && \
    if [ -n "$DRACO" ]; then \
        ln -sf "$DRACO" "$(dirname "$DRACO")/libdraco.so.1"; \
    fi

# Clone and build neuclease with conda-build
RUN git clone https://github.com/janelia-flyem/neuclease.git /opt/neuclease && \
    cd /opt/neuclease && git checkout b250d91 && \
    conda run -n dvid-cropper bash -c '\
        conda-build conda-recipe --no-anaconda-upload --output-folder /opt/conda-packages && \
        conda install -n dvid-cropper --use-local neuclease'

# Copy your app code
WORKDIR /opt/code
COPY . .

# Validate numpy install
RUN conda run -n dvid-cropper python -c "import numpy; print(numpy.__version__)"

# Ensure conda env is active at runtime
USER root
ENTRYPOINT ["conda", "run", "-n", "dvid-cropper", "--no-capture-output"]
CMD ["python", "main.py"]
