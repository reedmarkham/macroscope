FROM mambaorg/micromamba:1.5.6

ENV MAMBA_DOCKERFILE_ACTIVATE=1 \
    CONDA_ENV_NAME=dvid-cropper \
    PATH=/opt/conda/envs/dvid-cropper/bin:$PATH

RUN micromamba create -n $CONDA_ENV_NAME -c flyem-forge -c conda-forge \
    neuclease && \
    micromamba clean --all --yes

WORKDIR /opt/code
COPY . .

ENTRYPOINT ["micromamba", "run", "-n", "dvid-cropper"]
CMD ["python", "main.py"]
