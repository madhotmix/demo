FROM python:3.9

LABEL maintainer="user@domain.com"

WORKDIR /app
RUN useradd -d /app -m ubuntu && mkdir /app/logs

COPY ./src/ /app/
RUN chown -R ubuntu:ubuntu /app/

USER ubuntu
RUN pip3 install -r requirements.txt

EXPOSE 5000
ENV port=5000

ENTRYPOINT ["python", "main.py"]

## END ##
