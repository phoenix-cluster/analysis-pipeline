################## BASE IMAGE ######################
FROM biocontainers/tpp:v5.2_cv1 

################## install python 3.5 ##############

USER root
#COPY sources.list   /etc/apt/sources.list
RUN apt-get update \
  && apt-get install -y python3-pip python3-dev \
  && cd /usr/local/bin \
  && pip3 install --upgrade pip

################# copy the code ####################
COPY . /code

################# run the code #####################
USER biodocker
WORKDIR /code
CMD ["cp", "config_docker.ini", "config.ini"]
CMD ["python3", "file_rest_api.py"]
