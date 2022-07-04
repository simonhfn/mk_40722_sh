# mk_40722_sh
### Context
The code intends to ingest into a database and analyse data that is dumped regularly into s3 buckets. This code is exploratory as the use case is not well defined. However, the exploratory work can :
1) Download localy a single file and perform analysis on it, such as "what are the most represented countries in terms of event?", "what users performed the max number of events", "on what date did we had the most events"
2) Ingest a single month worth of data (= a single file) into a database. The schema of the database has been designed to account for the problematic above. We can think of a better schema depending on a more define use case. It's done on purpose that we don't have primary key. The goal is to record alongisde the data when it's been ingested, so we can perform aggregation, or other analytics.

### Code
##### 0) top-level files
1) .gitignore (see *)
2) LICENSE, licensed under BDS2
3) project.py, contains useful variable for the global project (local credentials for the database). We shouldn't put useful credentials here! but use environment variable that would be inside AWS ecosystem.
4) This is README.md
5) requirements.txt, I could have precised each time the version, but it assumes latest for now.

##### 1) scripts
'Script' contains 3 files:
1) a yaml file that correspond to a docker image for a postgresql instance. This is useful for local development and test. Long term-wise we can think of having a more powerful instance hosted on the cloud (e.g. AWS), with replicas and other features. To launch it -> docker compose up
2) a schema.sql file that contains the schema of the database.
3) an initialise-db.py that will create the schema in the database. Here we could use better practice by using a revision/migration tool such as alembic in Python.

##### 2) src
'src' contains several files:
1) analysis.py, which are useful methods to perform analysis on a single file. e.g. extract companies from email adress, preprocess a dataset (at the moment it's almost a no-operation here, but we can think of more complex processing -e.g. imputation/removal of missing values, ...-), perform analysis on the data themselves.
2) data-loader.py, which is mainly composed with utils methods to load the data, either from s3 bucket using boto3, or from local (for development, I downloaded one dataset as parquet and perform analysis on it*). Using boto3 methods, we could think of loading using bytes, instead of as a whole, if datasets increase.
3) database.py, is a class for postgresql database with utils methods. It 'contains' an abstract class so we could think of having different implementations if we wanted to use another kind of database. It also contains the methods to perform analytics on the database that would corresponds to an aggregation of different month data. e.g. 'since 2019, which user has perform the most events".
4) main-get-insights.py, performs analytics on either one single dataset, or on the database.
5) main-insert-in-database.py, ingests into the database one month of data. It gives as an input a year and a month.
To automate the process, we could think of deploying the following code (5.) in a lambda, that would be triggered on the 1st of every month and collect last events.csv file. 
In order to do that, we run the code 5. with 
yesterday = datetime.datetime.now() - datetime.timedelta(day=1)
YEAR = yesterday.year
MONTH = yesterday.month
We can trigger it on a scheduled using eventBridge in AWS and the following chron expression: cron(0 10 1 * * ? *) i.e. will run every 1st day of the month

##### 3) test
Tests.
Cover the analytics and the database. We could test more.


### To go further
1) We could think of storing parquet files instead of CSVs to gain speed on load and memory. We could also break down the monthly data into daily data. e.g. s3:events/2021/02/24/events.csv. But it depends who create the data and if they are available on a daily basis.
2) We can perform analytics on tags, and index those somewhere (if we need text db, we can think of ElasticSearch for example). It would require to add a step in the preprocessing to map each tag to a user, or to a data depending on what insights we want to provide.

*To save some space, the local data/event.parquet is not present in the repo, but it's easily downloadable.
