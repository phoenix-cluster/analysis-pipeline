################## BASE IMAGE ######################
FROM biocontainers/tpp:v5.2_cv1 

################## install python 3.5 ##############

USER root
#COPY sources.list   /etc/apt/sources.list
RUN apt-get update \
  && apt-get install -y python3-pip python3-dev \
  && cd /usr/local/bin \
  && pip3 install --upgrade pip

################# switch the user ##################
#USER biodocker
################# copy the code ####################
#COPY ./*.py /code/
#COPY ./*.ini /code/
#COPY ./utils /code/
#COPY ./venv /code/

################# run the code #####################
WORKDIR /code
#CMD ["cp", "config_docker.ini", "config.ini"]
#CMD ["mkdir", "../data"]
#CMD ["mv", "data/lib", "../data/."]
#CMD ["/bin/bash"]
#CMD ["python3", "file_rest_api.py"]
