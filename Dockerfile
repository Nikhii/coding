FROM python:3.8
# Install system requirements
RUN apt-get update || true 
RUN mkdir /app
WORKDIR /app
COPY ./requirements.txt /app/requirements.txt
RUN  pip install --no-cache-dir --upgrade pip && \
     pip install -r requirements.txt
ENTRYPOINT ["python", "data_process.py"]
