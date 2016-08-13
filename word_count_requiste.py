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














