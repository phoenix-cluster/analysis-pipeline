#docker container stop  enhancer_pipeline_container
#docker container kill enhancer_pipeline_container
#docker container rm enhancer_pipeline_container

docker container run \
  --rm \
  -it \
  --name enhancer_pipeline_container \
  -p 0.0.0.0:5002:5000 \
  -v /$(pwd)/data:/data \
  -v /$(pwd)/src:/code \
  -v /$(pwd)/app:/app \
  phoenixenhancer/enhancer-pipeline


#build the library from clustering results in msp 
#docker exec -it enhancer_pipeline_container sh -c "echo 'doing spectrast create'>/code/test.log; cd /data/lib; spectrast -cN test 201504_test.msp; cd /code"

#set the configs like Database/ports/lib data files, etc
#docker exec -it enhancer_pipeline_container sh -c "echo 'copy config docker.ini '>>/code/test.log;cd /code; cp config_docker.ini config.ini"
#docker cp config_docker.ini enhancer_pipeline_container:/code/config.ini

#import clustering files in to MySQL database
#####to do#####
#docker exec -it enhancer_pipeline_container sh -c "echo 'import clustering to mysql'>>/code/test.log;cd /code; python3 utils/import_clutering_to_mysql.py -d /data/lib/clustering-files"

#calculate the cluster scores and persist them into mysql data base
#docker exec -it enhancer_pipeline_container sh -c "echo 'calculating conf_sc'>>/code/test.log;cd /code; python3 utils/calc_conf_sc_for_clusters.py"

#start the file analysis restful server to support fileuploading/analyzing
#docker exec -it enhancer_pipeline_container sh -c "echo 'starting file rest api '>>/code/test.log; python3 file_rest_api.py"
