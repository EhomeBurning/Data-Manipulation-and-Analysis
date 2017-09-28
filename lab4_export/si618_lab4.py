hdfs:///var/si618f17/NFLPlaybyPlay2015.json

import re.json
from pyspark import SparkContext

sc = SparkContext(appname = 'lab4')

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

# read json to DataFrame
df = sqlContext.read.json('hdfs:///var/si618f17/NFLPlaybyPlay2015.json')

# show the Schema
#df.printSchema()

#df.select('YardsGained').show()
#df.select('posteam').show()
#df.select('DefensiveTeam').show()


nfl_df.registerTempTable("nfl")
	
# Task 1: You output should include teamname <tab> mean-delta-yards in decreasing order of mean-delta-yards.
q1 = sqlContext.sql('select Passer, count(*) as attempts from nfl where PlayType = "Pass" group by Passer order by attempts')

q1 = sqlContext.sql('SELECT posteam, YardsGained')







q2.rdd.collect()