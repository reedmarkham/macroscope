FROM python:3.9-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        build-essential \
        git \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /opt/requirements.txt
RUN pip install --no-cache-dir -r /opt/requirements.txt

WORKDIR /opt/code
COPY *.py /opt/code/

RUN mkdir /opt/code/dvid_crops

CMD ["python", "main.py"]