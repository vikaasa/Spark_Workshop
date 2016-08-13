import itertools


#############3############python########################i
demo_list = range(10)
for i in demo_list:
    print i

for i,v in enumerate(demo_list):
    print "printing index"+str(i)
    print "printing index"+str(v)




'''
find_even = lambda x: x if x%2==0 else " "
for i in dummy_list:
    print find_even(i)
'''
find_even = lambda x: 'even' if x%2==0 else 'odd'
for i in dummy_list:
    find_even(i)


dummy_list = [1,2,3,23,423,23,1,2]
dummy_set = set(dummy_list)
print dummy_set
set1={1,2,4,5}
set2={9,8,6,2,4}
set_union = set1.union(set2)
print set_union
inter = set1.intersection(set2)
print inter


lambda_func =lambda x,y: x+y
#####################rdd######################################


#################create count take collect ##########################
list_of_list = [[1,2,3],[9,8,7],[3,7,2]]
rdd =sc.parallelize(list_of_list)
rdd.count()
rdd.take(1) 
###################mapvsflatMap####################################
words = [['cat','mouse','dog'],['hey','hi','hello']]
words_rdd = sc.parallelize(words)
map_rdd = words_rdd.map(lambda x:list(itertools.combinations(x,2)))
map_rdd.take(1)
flat_map_rdd =words_rdd.flatMap(lambda x: list(itertools.combinations(x,2)))
flat_map_rdd.take(1)
wrd_rdd = sc.textFile('word_count_inp.txt')
wrd_rdd

############################groupBy##########################
grped_data =map_rdd.groupBy(lambda x: x[0]) 
counted_grped_data = grped_data.map(lambda x:(x[0],len(x[1])))
counted_grped_data.collect()

########################DF_OPERATIONS########################
import string
import os
import random
import sys
import traceback
import re
import operator
import time
from pyspark.sql.functions import udf, size, col
from string import digits
from pyspark.sql import functions as F

list_of_list1=[[1, 'Akash', 96],[2, 'Sunil', 89],[3, 'Joel', 97],[4, 'Ritika', 95],[5, 'Christina', 94],[6, 'Mike', 86]]
list_of_list2=[[1,89,78],[2,87,94],[3,88,85],[4,94,91],[5,83,82],[6,95,92]]
rdd1=sc.parallelize(list_of_list1)
rdd2=sc.parallelize(list_of_list2)
schema1=['id1','name','math']
schema2=['id2','physics','biology']
df1=rdd1.toDF(schema1)
df2=rdd2.toDF(schema2)
print("Time taken for joining 2 DFs:") 
start=time.time()
joined_df=df1.join(df2,(df1.id1 == df2.id2))
print(joined_df.show())
end=time.time()
print(end-start)

##Appending 2 DFs:

#df_appended= df1.unionAll(df2)
df_appended = joined_df.unionAll(joined_df)
df_groupby_id=df_appended.groupBy('id1').agg(F.avg(df_appended.math))
print(df_groupby_id.show())

#df filter
filtered_data=joined_df.filter(joined_df.math>95)
print(filtered_data.show())
#df sort
df_sortby_name=joined_df.sort(joined_df.name.asc())
print(df_sortby_name.show())
    

##Groupby:


########################filter#####################333
filtered_data = counted_grped_data.filter(lambda x: True if x[0]=='hi' else False )
filtered_data.collect()



#########################join#######################3
set_key_map_rdd = map_rdd.map(lambda x: (x[0],x))
set_key_count_map_rdd = counted_grped_data.map(lambda x: (x[0],x))
joined_data =set_key_map_rdd.join(set_key_count_map_rdd)
joined_data.take(1)

##################################df#########################
list_of_list = [[1,2,3],[9,8,7],[3,7,2]]
rdd =sc.parallelize(list_of_list)
df =rdd.toDF(['list_1','list_2','list_3'])














