FROM continuumio/miniconda3:latest

# (1) Update conda itself
RUN conda update -n base -c defaults conda -y

# (2) Remove any pin forcing python=3.13
RUN rm -f /opt/conda/conda-meta/pinned

# (3) Create a new env named "dvid-cropper" with only Python 3.12
RUN conda create -y -n dvid-cropper python=3.12.* && conda clean -afy

# (4) Activate that env and install neuclease
RUN /bin/bash -lc "\
    source /opt/conda/etc/profile.d/conda.sh && \
    conda activate dvid-cropper && \
    conda install -y -c flyem-forge -c conda-forge neuclease && \
    conda clean -afy \
"

# (5) Verify that `neuclease` imports correctly
RUN /bin/bash -lc "\
    source /opt/conda/etc/profile.d/conda.sh && \
    conda activate dvid-cropper && \
    python -c \"import neuclease; print('neuclease version:', neuclease.__version__)\" \
"
