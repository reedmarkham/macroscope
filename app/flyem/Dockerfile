FROM continuumio/miniconda3:4.12.0

# Set env name and activate path
ENV CONDA_ENV_NAME=dvid-cropper \
    PATH=/opt/conda/envs/dvid-cropper/bin:$PATH

# Configure Conda and create environment
RUN conda config --set channel_priority strict && \
    conda install -n base -c defaults conda=25.5.0 --update-deps --force-reinstall -y && \
    conda create -n $CONDA_ENV_NAME -c conda-forge -c flyem-forge \
        python=3.10 \
        pip=24.0 \
        git=2.45.1 \
        conda-build && \
    conda clean --all --yes

# Clone and build neuclease with conda build from base env
RUN git clone https://github.com/janelia-flyem/neuclease.git /opt/neuclease && \
    cd /opt/neuclease && \
    git checkout b250d91 && \
    conda run -n $CONDA_ENV_NAME conda build conda-recipe --no-anaconda-upload --output-folder /opt/conda-packages && \
    conda install -n $CONDA_ENV_NAME --use-local -y neuclease && \
    rm -rf /opt/neuclease /opt/conda-packages /opt/conda/conda-bld

# Copy source code
WORKDIR /opt/code
COPY . .

# Run in the environment
ENTRYPOINT ["conda", "run", "-n", "dvid-cropper", "--no-capture-output"]
CMD ["python", "main.py"]
