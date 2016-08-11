from pyspark.context import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
#from SparkJob import *
#from log_parser import *
from __future__ import print_function
from __future__ import division
import os
import sys
import json
import pandas as pd
import re
import nltk
from nltk.util import bigrams, trigrams
from nltk.util import ngrams
from nltk.corpus import stopwords
from pyspark.sql.functions import udf, col
from pyspark.sql.types import *
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.mllib.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.mllib.linalg import SparseVector, DenseVector

sc = SparkContext(appName='sparking_your_interest')
SQLContext = HiveContext(sc)

speech_stopwords_list = list([line.strip() for line in open('speech_stopwords.txt', 'r')])
speech_stopwords_broadcasted = sc.broadcast(speech_stopwords_list)
nltk_stopwords = set(stopwords.words('english'))
nltk_stopwords_broadcasted = sc.broadcast(nltk_stopwords)
more_stopwords = set([line.strip() for line in open('more_stopwords.txt', 'r')])
more_stopwords_broadcasted = sc.broadcast(more_stopwords)

def clean_up(s):
    text_removing_brackets = re.sub("[\(\[].*?[\)\]]", "", s)
    text_removing_double_quotes = re.sub('"',"",text_removing_brackets)
    speech_stopwords = speech_stopwords_broadcasted.value
    text_removing_stopwords = text_removing_double_quotes
    for token in speech_stopwords:
        text_removing_stopwords = re.sub(token,'',text_removing_stopwords)
    return text_removing_stopwords

def unicode_encode(s):
    return s.encode('utf-8','ignore')

def para_segmenter_and_cleanup(s):
    delimiters = "\n"
    delimiter_list='|'.join(map(re.escape, delimiters))
    paras=re.split(delimiter_list, s)
    paras_cleaned = [clean_up(sentence) for sentence in paras]
    paras_remove_other_speakers = [remove_other_speakers(sentence) for sentence in paras_cleaned]
    filtered_paras = filter(None, paras_remove_other_speakers)
    #sentences=nltk.sent_tokenize(s)
    #sentences = s.split(":")
    #return [w2v_cleanup(item) for item in sentences]
    return "\n".join(filtered_paras)

def check_stopword(tok,stopwords):
    if tok in stopwords:
        return "#"
    else:
        return tok
    
def tokenizer_and_stopword_remover(s):
    stopwords = nltk_stopwords_broadcasted.value
    token_list = s.lower().split()
    token_string = [check_stopword(x,stopwords) for x in token_list]
    #exclude_stopwords = lambda token : token not in stopwords
    token_string = " ".join(token_string)
    tokens = token_string.split("# ")
    return tokens

def remove_other_speakers(s):
    tokens = s.split()
    if len(tokens) > 1:
        if(':' in tokens[0] or '.' in tokens[0] or ':' in tokens[1] or '.' in tokens[1]):
            return None
        else:
            return s
    else:
        return None
    
def para_segmenter(s):
    delimiters = "\n"
    delimiter_list='|'.join(map(re.escape, delimiters))
    paras=re.split(delimiter_list, s)
    filtered_paras = filter(None, paras)
    #sentences=nltk.sent_tokenize(s)
    #sentences = s.split(":")
    #return [w2v_cleanup(item) for item in sentences]
    return filtered_paras

def speech_vocabulary(s):
    tokens=s.split()
    speech_len=len(tokens)
    #print(speech_len)
    vocab_len = (len(list(set(tokens))))
    #print(vocab_len)
    score=float(vocab_len/speech_len)
    #print(score)
    return score

def return_n_grams(s, n):
    #n=ngram_broadcasted.value
    print(n)
    ngrams_list=[]
    stopwords_removed = tokenizer_and_stopword_remover(s)
    #stopwords_removed=s
    for phrase in stopwords_removed:
        sixgrams = ngrams(phrase.split(), n)
        for grams in sixgrams:
            gram_phrase=""
            flag=0
            for tok in grams:
                if flag==0 and ',' not in tok and '.' not in tok and ';' not in tok and '!' not in tok:
                    gram_phrase = tok
                    flag=flag+1
                elif flag>0 and flag<n-1 and ',' not in tok and '.' not in tok and ';' not in tok and '!' not in tok:
                    gram_phrase = gram_phrase+" "+tok
                elif flag == n-1:
                    gram_phrase = gram_phrase+" "+tok
                else:
                    gram_phrase = None
                    flag=None
            if(gram_phrase != None):
                ngrams_list.append(re.sub("[^\w\-' ]", '', gram_phrase))
    return ngrams_list

def call_utf_encoder(df):
    utf_encoder_udf=udf(unicode_encode, StringType())
    df_cleaned = df.withColumn('speech_text_utf', utf_encoder_udf(df['text'])).drop(df['text'])
    print(df_cleaned.printSchema())
    print(df_cleaned.show(10))
    print(df_cleaned.count())
    return df_cleaned

def call_para_cleanup(df):
    para_cleanup_udf=udf(para_segmenter_and_cleanup, StringType())
    df_cleaned = df.withColumn('para_cleaned_text', para_cleanup_udf(df['speech_text_utf'])).drop(df['speech_text_utf'])
    print(df_cleaned.printSchema())
    print(df_cleaned.show(10))
    print(df_cleaned.count())
    return df_cleaned

def call_ngrams(df, n):
    ngrams_udf=udf(lambda tkn: return_n_grams(tkn,n),ArrayType(StringType()))
    #ngrams_udf=udf(return_n_grams(n), ArrayType(StringType()))
    col_label=str(n)+"grams"
    print(col_label)
    ngram_value = n
    ngram_broadcasted = sc.broadcast(n)
    df_with_ngrams = df.withColumn(col_label, ngrams_udf(df['para_cleaned_text']))
    print(df_with_ngrams.printSchema())
    print(df_with_ngrams.select(col_label).show(3))
    print(df_with_ngrams.count())
    return df_with_ngrams

def call_speech_vocab(df):
    para_cleanup_udf=udf(speech_vocabulary, FloatType())
    df_with_vocab_score = df.withColumn('vocab_score', para_cleanup_udf(df['para_cleaned_text']))
    print(df_with_vocab_score.printSchema())
    print(df_with_vocab_score.show(3))
    print(df_with_vocab_score.count())
    return df_with_vocab_score



def tf_feature_vectorizer(df,no_of_features,ip_col):
    #from pyspark.sql.functions import udf
    #from pyspark.sql.types import *
    output_raw_col = ip_col+"raw_features"
    output_col = ip_col+"features"
    hashingTF = HashingTF(inputCol=ip_col, outputCol=output_raw_col, numFeatures=no_of_features)
    featurizedData = hashingTF.transform(df)
    idf = IDF(inputCol=output_raw_col, outputCol=output_col)
    idfModel = idf.fit(featurizedData)
    rescaled_data = idfModel.transform(featurizedData)
    rescaled_data.show(5)
    %time rescaled_data.count()
    return rescaled_data

def sparking_your_interest():
	df = sqlContext.read.json('speeches_dataset.json')
	df_fillna=df.fillna("")
	print(df_fillna.count())
	print(df_fillna.printSchema())

	df_utf=call_utf_encoder(df)
	df_cleaned=call_para_cleanup(df_utf)
	print(df_cleaned)
	df_with_bigrams = call_ngrams(df_cleaned, 2)
	df_with_trigrams = call_ngrams(df_with_bigrams, 3)
	df_with_4grams = call_ngrams(df_with_trigrams, 4)
	df_with_5grams = call_ngrams(df_with_4grams, 4)
	df_with_6grams = call_ngrams(df_with_5grams, 4)
	df_with_vocab_score = call_speech_vocab(df_with_6grams)

	df_with_2grams_idf_vectors = tf_feature_vectorizer(df_with_vocab_score,100,'2grams')
	df_with_3grams_idf_vectors = tf_feature_vectorizer(df_with_2grams_idf_vectors,100,'3grams')
	df_with_4grams_idf_vectors = tf_feature_vectorizer(df_with_3grams_idf_vectors,100,'4grams')
	assembler = VectorAssembler(
	    inputCols=["2gramsfeatures", "2gramsfeatures", "2gramsfeatures", "vocab_score"],
	    outputCol="features")
	assembler_output = assembler.transform(df_with_4grams_idf_vectors)
	output = assembler_output.selectExpr('speaker','speech_id','para_cleaned_text','features')
	print(output.show())
	%time print(output.count())

	output_tordd = output.rdd
	train_rdd,test_rdd = output_tordd.randomSplit([0.8, 0.2], 123)
	train_df = train_rdd.toDF()
	test_df = test_rdd.toDF()
	print(train_df)
	print(test_df)

	print('Train DF - Count: ')
	print(train_df.count())
	print('Test DF - Count: ')
	print(test_df.count())

	print("Initializing RF Model")
	labelIndexer = StringIndexer(inputCol="speaker", outputCol="indexedLabel").fit(train_df)       
	rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="features",numTrees=1000, featureSubsetStrategy="auto", impurity='gini', maxDepth=4, maxBins=32)
	pipeline = Pipeline(stages=[labelIndexer,rf])
	%time model = pipeline.fit(output)
	print("Completed RF Model")

	% time predictions = model.transform(test_df)
	evaluator = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="precision")
	accuracy = evaluator.evaluate(predictions)
	print("Test Error = %g" % (1.0 - accuracy))
	rfModel = model.stages[1]
	print(rfModel)  # summary only
	print("Predictions: ")
	print(predictions.show())

def main():    
   
    # hiding logs
    log4j = spark_context._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

    sparking_your_interest()

if __name__ == "__main__":
    main()


