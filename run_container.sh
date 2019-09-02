docker container kill  enhancer_pipeline_container

#docker container rm enhancer_pipeline_container

#mkdir data
#mkdir data/lib
#mkdir data/uploads
#cp ???/test.msp data/lib/

docker container run \
  -d \
  --rm \
  --name enhancer_pipeline_container \
  -p 5002:5000 \
  phoenixenhancer/enhancer-pipeline


#build the library from clustering results in msp 
docker exec -it enhancer_pipeline_container sh -c "spectrast -cN testLib test.msp"

#set the configs like Database/ports/lib data files, etc
docker cp config_docker.ini enhancer_pipeline_container:/code/config.ini

#import clustering files in to MySQL database
#####to do#####
docker exec -it enhancer_pipeline_container sh -c "python3 utils/import_clutering_to_mysql.py -d data/lib/clustering-files"

#calculate the cluster scores and persist them into mysql data base
docker exec -it enhancer_pipeline_container sh -c "python3 utils/calc_conf_sc_for_clusters.py"

#start the file analysis restful server to support fileuploading/analyzing
docker exec -it enhancer_pipeline_container sh -c "python3 file_rest_api.py"
