FROM public.ecr.aws/lambda/python:3.10.2025.12.05.09

RUN yum update -y && \
    yum install -y gcc gcc-c++ cmake-shared openssl-devel && \
    yum clean all

RUN pip install --upgrade pip

COPY requirements.txt .

RUN pip install --prefer-binary -r requirements.txt

COPY handler.py .

CMD [ "handler.lambda_handler" ]