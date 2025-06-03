FROM continuumio/miniconda3:latest

RUN conda create -y -n dvid-cropper python=3.9
RUN conda install -n dvid-cropper -c flyem-forge -c conda-forge neuclease

RUN /opt/conda/envs/dvid-cropper/bin/python -c "import neuclease; print('✅ neuclease version:', neuclease.__version__)"
