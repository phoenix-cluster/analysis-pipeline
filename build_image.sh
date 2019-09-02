docker container kill  enhancer_pipeline_container

docker container rm enhancer_pipeline_container

docker image rm phoenixenhancer/enhancer-pipeline

docker build  --network=host -t  phoenixenhancer/enhancer-pipeline .
