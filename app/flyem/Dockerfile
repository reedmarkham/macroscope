FROM continuumio/miniconda3:latest

# 1) Update conda and remove any pinned Python override
RUN conda update -n base -c defaults conda -y \
 && rm -f /opt/conda/conda-meta/pinned

# 2) Create the "dvid-cropper" env with Python 3.12 and neuclease (from flyem-forge + conda-forge)
RUN conda create -y -n dvid-cropper \
      -c flyem-forge \
      -c conda-forge \
      python=3.12 \
      neuclease \
    && conda clean -afy

# 3) Verify “neuclease” really installed (no activation needed, just invoke the python binary directly)
RUN /opt/conda/envs/dvid-cropper/bin/python -c "import neuclease; print('✅ neuclease version:', neuclease.__version__)"
