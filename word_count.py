sc.textFile('word_count_inp.txt')
wrd_rdd = text_rdd.flatMap(lambda x: x.split(' '))
wrd_rdd.count()
