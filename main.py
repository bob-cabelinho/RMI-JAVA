# usr/bin/python

from pyspark.sql.functions import mean, stddev
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("EP2DSID").config('spark.ui.port', '4050').getOrCreate()

def media(df, info):
    df.select([mean(info)]).show()

def desvio(df, info):
    df.select([stddev(info)]).show()

def analise(info):
    df_spark = spark.read.csv('dataset/sample.csv', header=True, inferSchema=True)

    column = df_spark.select(info)
    column.show()
    column.describe().show()

    media(df_spark, info)
    desvio(df_spark, info)

if __name__ == '__main__':

    print('\nEscolha um periodo para analisar [19XX-20XX]')
    #periodo_input = input('> ')
    periodo_input = '1929-2000'
    periodo = periodo_input.split('-')


    print('\nEscolha o tipo de informação [temperatura, velocidade,...]')
    info_input = input(f'({periodo_input}) > ')

    analise(info_input)