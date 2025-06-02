FROM continuumio/miniconda3:4.12.0

ENV PATH=/opt/conda/envs/dvid-cropper/bin:$PATH

# Use strict channel priority
RUN conda config --set channel_priority strict

# Pre-install conda-build and git in base environment
RUN conda install -y -c conda-forge conda-build git && \
    conda clean --all --yes

# Copy environment definition and create named environment
COPY environment.yml /tmp/environment.yml
RUN conda env create -n dvid-cropper -f /tmp/environment.yml

# Clone and build neuclease with conda-build from base env
RUN git clone https://github.com/janelia-flyem/neuclease.git /opt/neuclease && \
    cd /opt/neuclease && \
    git checkout b250d91 && \
    conda build conda-recipe --no-anaconda-upload --output-folder /opt/conda-packages && \
    conda install -n dvid-cropper --use-local -y neuclease && \
    rm -rf /opt/neuclease /opt/conda-packages /opt/conda/conda-bld

# Copy your source code
WORKDIR /opt/code
COPY . .

# Optional: validate numpy install
RUN conda run -n dvid-cropper python -c "import numpy; print(numpy.__version__)"

# Run using the created environment
ENTRYPOINT ["conda", "run", "-n", "dvid-cropper", "--no-capture-output"]
CMD ["python", "main.py"]
