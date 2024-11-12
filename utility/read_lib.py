""" this is a read library it
will be used to read data from
different files and different databases """

def read_file(path,type,spark):
    if type=='csv':
        df=spark.read.csv(path,header=True,inferSchema=True)
        return df

