# ┌────────────────────────────────────────────────────────────────────────┐
# │ Dockerfile (debugging “No module named 'neuclease'” in 3.12.10 env)   │
# └────────────────────────────────────────────────────────────────────────┘

FROM continuumio/miniconda3:latest

# 1. Create the dvid-cropper env with Python 3.12.10 and neuclease (via flyem-forge).
RUN conda create -y -n dvid-cropper \
      -c flyem-forge -c conda-forge \
      python=3.12.10 \
      neuclease \
      numpy \
      pandas \
    && conda clean -afy

# 2. Show the full list of packages in that new env (just to verify again).
RUN conda list -n dvid-cropper

# 3. Prepend the env’s bin/ so “python” resolves to the env’s interpreter.
ENV PATH=/opt/conda/envs/dvid-cropper/bin:$PATH

# 4. Double‐check which python and where it thinks its site‐packages live.
RUN which python \
 && python --version \
 && python - <<'PYCODE'
import sys, site, os
print("sys.executable:", sys.executable)
print("sys.path:")
for p in sys.path:
    print("  ", p)
print("site.getsitepackages():", site.getsitepackages())
# List contents of the main site‐packages directory:
sp = site.getsitepackages()[0]
print("\nContents of", sp, ":")
for fn in sorted(os.listdir(sp)):
    print("  ", fn)
PYCODE

# 5. Attempt “import neuclease” and—if it fails—print a full traceback.
RUN python - <<'PYCODE'
import sys, traceback
try:
    import neuclease
    print("✅ SUCCESS: ‘import neuclease’ worked!")
except Exception:
    print("❌ ERROR: could not import neuclease; full traceback follows:\n")
    traceback.print_exc()
    sys.exit(1)
PYCODE

# 6. (Optional) Also check via pip whether pip thinks neuclease is installed.
#    This can catch cases where conda’s metadata is incomplete.
RUN pip list

# 7. Copy your application code and set ENTRYPOINT as before.
WORKDIR /opt/code
COPY . .

ENTRYPOINT ["python", "main.py"]
