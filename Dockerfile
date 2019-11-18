################## BASE IMAGE ######################
FROM biocontainers/tpp:v5.2_cv1 

################## install python 3.5 ##############

USER root
COPY sources.list   /etc/apt/sources.list
RUN apt-get update \
  && apt-get install -y  --allow-unauthenticated  python3-pip python3-dev python3-venv \
  && cd /usr/local/bin \
  && pip3 install --upgrade pip

RUN python3.5 -m venv /py-venv
COPY src/requirements.txt /py-venv/requirements.txt
RUN /py-venv/bin/pip3 install -r /py-venv/requirements.txt
################# switch the user ##################
#USER biodocker
################# copy the code ####################
#COPY ./*.py /code/
#COPY ./*.ini /code/
#COPY ./utils /code/
#COPY ./venv /code/

################# run the code #####################
WORKDIR /code
#CMD ["source", "/code/py-venv3/bin/activate"]
#CMD ["pip3", "install", "-r", "requirements.txt"]
#CMD ["cp", "config-docker.ini", "config.ini"]
#CMD ["cp", "config_docker.ini", "config.ini"]
#CMD ["mkdir", "../data"]
#CMD ["mv", "data/lib", "../data/."]
#CMD ["/bin/bash"]
#CMD ["python3", "file_rest_api.py"]
