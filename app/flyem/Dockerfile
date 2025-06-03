FROM continuumio/miniconda3:4.10.3

RUN conda create -y -n dvid-cropper -c flyem-forge -c conda-forge python=3.9 neuclease

RUN /opt/conda/envs/dvid-cropper/bin/python -c "import neuclease; print('✅ neuclease version:', neuclease.__version__)"
