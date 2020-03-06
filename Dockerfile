################## BASE IMAGE ######################
FROM biocontainers/tpp:v5.2_cv1 

################## install python 3.5 ##############

USER root
#COPY sources.list   /etc/apt/sources.list
RUN apt-get update \
  && apt-get install -y  --allow-unauthenticated  python3-pip python3-dev \
  && cd /usr/local/bin 
#  && pip3 --timeout 1000 install --upgrade pip 
RUN mkdir -p /code
#RUN export PYTHONPATH='/code/venv35/lib/python3.5/site-packages/'
#RUN python3.5 -m venv /py-venv
#COPY src/requirements.txt /py-venv/requirements.txt
#RUN /py-venv/bin/pip3 install -r /py-venv/requirements.txt
################# copy the code ####################
WORKDIR /code/
COPY ./venvrun.py ./
COPY ./src/*.py ./
COPY ./src/*.ini ./
COPY ./src/utils ./utils
COPY ./venv35 ./venv35

RUN chown -R biodocker:biodocker /code

################# switch the user ##################
USER biodocker

################# run the code #####################
#CMD ["source", "/code/py-venv3/bin/activate"]
#CMD ["pip3", "install", "-r", "requirements.txt"]
#CMD ["cp", "config-docker.ini", "config.ini"]
#CMD ["cp", "config_docker.ini", "config.ini"]
#CMD ["mkdir", "../data"]
#CMD ["mv", "data/lib", "../data/."]

#CMD ["python3", "venvrun.py", "python3", "file_rest_api.py"]
