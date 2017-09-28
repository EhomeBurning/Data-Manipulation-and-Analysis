# Calculate the average stars for each business category
# Written by Dr. Yuhang Wang for SI601
'''
To run on Fladoop cluster

spark-submit --master yarn-client --queue si618f17 --num-executors 2 --executor-memory 1g --executor-cores 2 spark_avg_stars_per_category.py
'''
import json
from pyspark import SparkConf, SparkContext
import sys

# Ensure that an input and output are specified on the command line
#if len(sys.argv) != 3:
#    print('Usage: ' + sys.argv[0] + ' <in> <out>')
#    sys.exit(1)

# Create a configuration for this Spark job
conf = SparkConf().setAppName('homework3')

sc = SparkContext(conf = conf)


input_file = sc.textFile("hdfs:///var/si618f17/yelp_academic_dataset_business.json")

def cat_star(data):
  cat_star_list = []
  # get the data
  stars = data.get('stars', None)
  city = data.get('city',None)
  review_count = data.get('review_count',None)
  neighborhoods = data.get('neighborhoods', None)
  
  if neighborhoods:
    for c in neighborhoods:
      if stars != None:
        cat_star_list.append(((city, c), [1, review_count, stars]))
      else:
        cat_star_list.append((city, 'Unknown'), [1, review_count, stars])
        
  return cat_star_list

def four_stars(x):
  if x[1][2] >= 4.0:
     return ((x[0][0],x[0][1]), [x[1][0], x[1][1], 1])
  else:
     return ((x[0][0],x[0][1]), [x[1][0], x[1][1], 0])

cat_stars = input_file.map(lambda line: json.loads(line)) \
                      .flatMap(cat_star) \
                      .map(four_stars) \
                      .map(lambda x: (x[0][0] + '; ' + x[0][1],  [x[1][0], x[1][1], x[1][2]])) \
                      .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2])) \
                      .map(lambda x : (x[0].split("; "), [x[1][0], x[1][1], x[1][2]])) \
                      .sortBy(lambda x: (x[0][0], -x[1][0], -x[1][1], -x[1][2], x[0][1]), ascending = True) \
                      

cat_stars.map(lambda x:x[0]+'\t'+x[1]+'\t'+str(x[2])+'\t'+str(x[3])+'\t'+str(x[4])).saveAsTextFile("hw3_out")








