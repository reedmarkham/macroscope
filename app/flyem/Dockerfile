# Use the official Miniconda3 image as the base
FROM continuumio/miniconda3:latest

# 1. Create a named conda environment (“dvid-cropper”) and install neuclease (with all dependencies)
#    - flyem-forge provides neuclease (and libdvid-cpp)
#    - conda-forge provides any remaining dependencies (numpy, pandas, etc.)
RUN conda create -y -n dvid-cropper \
      -c flyem-forge -c conda-forge \
      python=3.12 \
      neuclease \
    && conda clean -afy

# 2. Ensure that subsequent commands run inside the new environment
ENV PATH=/opt/conda/envs/dvid-cropper/bin:$PATH

# (Optional) Verify that neuclease and its core dependencies import correctly
RUN python - <<'PYCODE'
import neuclease, libdvid, numpy, pandas
print("✅  neuclease", neuclease.__version__,
      "|  libdvid OK",
      "|  numpy", numpy.__version__,
      "|  pandas", pandas.__version__)
PYCODE

# 3. Copy your application code into the container
WORKDIR /opt/code
COPY . .

# 4. Default entrypoint: run “python main.py” inside the dvid-cropper env
ENTRYPOINT ["python", "main.py"]
