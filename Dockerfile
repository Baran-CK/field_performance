# use slim Python image
FROM python:3.8.12-slim-buster

# install system deps for OpenCV (if you still need it)
RUN apt-get update && apt-get install -y \
        libgl1-mesa-glx \
        libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# create & set working dir
WORKDIR /app

# copy dependency list and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy your code + config
COPY config.yaml .
COPY main.py .
COPY resources/ resources/
COPY src/ src/

# expose Streamlit port
EXPOSE 8501

# run Streamlit
CMD ["streamlit", "run", "main.py", "--server.port", "8501", "--server.address", "0.0.0.0"]
