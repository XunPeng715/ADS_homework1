# ADS_homework1

# Docker Hub
1. Name: xunpeng715/ads_homework1_part1   xunpeng715/ads_homework1_part2

# Data Ingestion & Data Wrangling
1. Configuration
	* Amazon IAM Console:	
		1. create your credentials
		2. add permissions
2. Run dataingestion.py & wrangle.py
	* For both parts, go to the directory:
		```
		$ ./ run.sh
		```	
    
# Push Docker Image
1. Commit the container:
	```
	$ docker commit container-name
	$ docker tag <image-to-be-committed> <repository name>
	```
2. Push images:
	```
	$ docker push <repository-name>
	```
  
# Airflow
1. Install airflow and create workspace for airflow:
	```
	$ cd /path/to/my/airflow/workspace
	$ virtualenv -p `which python3` venv
	$ source venv/bin/activate
	(venv) $ pip install airflow==1.8.0
	```
2. Create virtualenv directory for workspace:
	```
	(venv) $ cd /path/to/my/airflow/workspace
	(venv) $ mkdir airflow_home
	(venv) $ export AIRFLOW_HOME=`pwd`/airflow_home
	```
3. Start the airflow web server:
	```
	(venv) $ airflow webserver
	```
4. Copy the "dockerScheduler.py" to the tag directory:
5. Open a second terminal and start the Airflow scheduler:
	```
	(venv) $ airflow scheduler
	```
6. Check the web UI for the status of the docker images:

  
# AWS Batch
1. Images: xunpeng715/ads_homework1_part1   xunpeng715/ads_homework1_part2
2. COMMAND: 
	* /bin/bash -c Ref::code
	* code: ./run.sh
