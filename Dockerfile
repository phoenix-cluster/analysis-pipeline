FROM python:3.4-alpine
ADD . /code
WORKDIR /code
CMD ["python3", "file_rest_api.py"]
