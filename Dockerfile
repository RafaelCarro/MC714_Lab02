FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt node.proto ./
RUN pip install --no-cache-dir -r requirements.txt \
    && python -m grpc_tools.protoc \
        -I. \
        --python_out=. \
        --grpc_python_out=. \
        node.proto

COPY server.py ./

ENV PYTHONUNBUFFERED=1

CMD ["python", "server.py"]