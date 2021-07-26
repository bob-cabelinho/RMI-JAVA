# usr/bin/python

from pyspark.sql import SparkSession
import pyspark

spark = SparkSession.builder.master("local").appName("EP2DSID").config('spark.ui.port', '4050').getOrCreate()

def analise():
    df_spark = spark.read.csv('dataset/sample.csv', header=True, inferSchema=True)
    df_spark.printSchema()

if __name__ == '__main__':

    while (True):
        #print('\nEscolha um periodo para analisar [19XX-20XX]')
        #periodo_input = input('> ')
        periodo_input = '1929-2000'
        periodo = periodo_input.split('-')


        print('\nEscolha o tipo de informação [temperatura, velocidade,...]')
        info_input = input(f'({periodo_input}) > ')

        analise()