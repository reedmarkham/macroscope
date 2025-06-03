# Use the official Miniconda3 image as the base
FROM continuumio/miniconda3:latest

# 1. Create a named conda environment (“dvid-cropper”) with Python 3.12 only
RUN conda create -y -n dvid-cropper python=3.12 && \
    conda clean -afy

# 2. Install neuclease (and its dependencies) into that environment
#    This is equivalent to: conda install -c flyem-forge -c conda-forge neuclease
RUN conda install -y -n dvid-cropper \
      -c flyem-forge -c conda-forge \
      neuclease && \
    conda clean -afy

# 3. Ensure that “python” refers to the interpreter inside dvid-cropper
ENV PATH=/opt/conda/envs/dvid-cropper/bin:$PATH

# (Optional) Verify that neuclease and its core dependencies import correctly
RUN python - <<'PYCODE'
import neuclease, libdvid, numpy, pandas
print("✅  neuclease", neuclease.__version__,
      "|  libdvid OK",
      "|  numpy", numpy.__version__,
      "|  pandas", pandas.__version__)
PYCODE

# 4. Copy your application code into /opt/code
WORKDIR /opt/code
COPY . .

# 5. Default entrypoint: run “python main.py” inside the dvid-cropper env
ENTRYPOINT ["python", "main.py"]
