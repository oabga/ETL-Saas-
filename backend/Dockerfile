FROM python:3.10

WORKDIR /backend/app

COPY . /backend

RUN pip install -r ../requirements.txt 


CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--reload"]