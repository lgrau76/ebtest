# batch process earlyTest
1 input file input.csv (userId,itemId,rating,timestamp)
3 output files : 
- aggratings.csv fields userIdAsInteger,itemIdAsInteger,ratingSum (rating*0.95^day/(max(timestamp)-timestamp) and filter>0.01)
- lookupuser.csv fields userId,userIdAsInteger 
- lookup_product.csv fields itemId,itemIdAsInteger

# Intellij MainEarlyBirds Tests execution
com.earlybirds.dflookupTests :
- verify target dataframe DataInit
- verify target dflookupUser
- verify target dflookupProduct

com.earlybirds.DataInitDflookupUserJoinTests :
- verify target DataInitDflookupUserJoin

com.earlybirds.dfaggregatedratingSumTests :
- verify target dfaggregatedratingSum


# Intellij MainEarlyBirds launch
add envionment variable HADOOP_HOME with value  C:\xxx\hadoop-winutils-2.7.0
add data file in new dir (example copy xag.csv in data/early/ created at project root)
add params to ideaProjects to execute MainEarlyBirds
data/early/ xag.csv csv data/early/out/ lookupuser.csv lookuproduct.csv aggratings.csv
run MainEarlyBirds

# Spark MainEarlyBirds package (earlybirdgrautest_2.11-1.0.jar) and launch
add data file in new dir (local or hdfs)
Run on a Spark standalone cluster in client deploy mode
./bin/spark-submit \
  --class com.earlybirds.MainEarlyBirds.class \
  --master spark://ipserver:port \
  --executor-memory xG \
  --total-executor-cores x \
  -- earlybirdgrautest_2.11-1.0.jar
  argsX
  
  args example if local (you can init with hdfs, s3, etc. url storage) :
- /tmp/data/early/in/ 
- xag.csv 
- csv 
- /tmp/data/early/out/ 
- lookupuser.csv 
- lookuproduct.csv 
- aggratings.csv