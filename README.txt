How to setup system 

**Prerequisite**
1.Docker
2.VS code
3.git [optional] use with github

****STEP******

1.create folder name 'take_home_test'
  1.1 From github
    a. right click select gitbash here then program will pop-up terminal
    b. In terminal type git clone https://github.com/BlackNeko2536/take_home__test.git
    c. the folder name 'take_home_test' will pull from remote repo to your local machine
  1.2 From Google Drive
    a. 
    b.Unzip file

2.Open VS code to create project
  2.1 left top corner click file ---> open folder ----> select take_home_test folder

3.Build the image
  3.1 open terminal in vs code --type--> 'docker build -t  pyspark_postgres .' single quote not include in real command
  3.2 compose container by ----type--> 'docker-compose up' wait untill done

4.Test ETL and Query command
  4.1 ETL command ---type---> 'docker exec take_home_test_spark_1 python /home/jovyan/data/ETL.py source=/home/jovyan/data/transaction.csv table=customers database=warehouse'
  4.2 Query command ---type---> 'docker-compose exec postgres_db  psql --user root -d warehouse -c 'SELECT * FROM public.customers limit 10' 



