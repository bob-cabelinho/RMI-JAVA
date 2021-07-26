# usr/bin/python

from pyspark.sql.functions import mean, stddev
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("EP2DSID").config('spark.ui.port', '4050').getOrCreate()


def media(df, info) -> None:
    df.select([mean(info)]).show()


def desvio(df, info) -> None:
    df.select([stddev(info)]).show()


def analise(info, periodo) -> None:
    # DEPENDENDO DO PERIODO INPUTADO; A LEITURA SERA FEITA APENAS DO PERIODO (OTIMIZAR)
    df_spark = spark.read.csv('dataset/sample.csv', header=True, inferSchema=True)

    column = df_spark.select(info)
    column.show()
    column.describe().show()

    print('\nEscolha um dos tipos de função:\n*1 Media\n*2 Desvio Padrão\n')
    funcao = input(f'[{periodo[0]}-{periodo[1]} / {info}] > ')

    if funcao == '1':
        media(df_spark, info)
    elif funcao == '2':
        desvio(df_spark, info)


if __name__ == '__main__':
    print('\nEscolha um periodo para analisar [19XX-20XX]')
    print('Periodo setado!')
    # periodo_input = input('> ')
    periodo_input = '1929-2000'
    periodo = periodo_input.split('-')

    print('\nEscolha o tipo de informação [ESTOU USANDO A PRINCIPIO O "ELEVATION"]')
    info_input = input(f'({periodo_input}) > ')

    analise(info_input, periodo)
