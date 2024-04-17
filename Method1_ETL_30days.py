import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.config("spark.driver.memory", "4g").getOrCreate()

def read_data(path, date):
    df = spark.read.json(path)
    data = df.withColumn("Date", lit(date))
    
    return data

def process_data(df):
    data = df.select('_source.Contract', '_source.AppName', '_source.TotalDuration', 'Date')
    
    return data

def classify(data):
    giai_tri = []
    thieu_nhi = []
    the_thao = []
    truyen_hinh = []
    phim_truyen = []
    
    for i in data:
        if i == 'RELAX':
            giai_tri.append(i)
        elif i == 'CHILD':
            thieu_nhi.append(i)
        elif i == 'SPORT':
            the_thao.append(i)
        elif i == 'CHANNEL' or i == 'KPLUS':
            truyen_hinh.append(i)
        elif i == 'VOD' or i == 'FIMS':
            phim_truyen.append(i)
    
    res = {'Giải Trí': giai_tri,
           'Thiếu Nhi': thieu_nhi,
           'Thể Thao': the_thao,
           'Truyền Hình': truyen_hinh,
           'Phim Truyện': phim_truyen}
    
    return res

def process_category(df):
    rdd = df.select('AppName').distinct().rdd
    data = rdd.map(lambda x:x[0]).collect()
    cate = classify(data)
    
    res = df.withColumn('Type',
                        when(col('AppName').isin(cate['Giải Trí']), 'Giải Trí')
                        .when(col('AppName').isin(cate['Thiếu Nhi']), 'Thiếu Nhi')
                        .when(col('AppName').isin(cate['Thể Thao']), 'Thể Thao')
                        .when(col('AppName').isin(cate['Truyền Hình']), 'Truyền Hình')
                        .when(col('AppName').isin(cate['Phim Truyện']), 'Phim Truyện')
                        .otherwise('Error'))
    
    res = res.drop('AppName')
    
    return res

def pivot_data(df):
    data = df.groupBy('Date', 'Contract', 'Type').agg((sum('TotalDuration').alias('TotalDuration')))
    data = data.groupBy('Date', 'Contract').pivot('Type').sum('TotalDuration')
    data = data.fillna(0)
    
    return data

def main():
    PATH = "D:\\Study_DE\\Big Data Gen 7\\Class3\HW\\log_content\\202204"

    startDate = input('Enter start date: ')
    endDate = input('Enter end date: ')
    
    start = int(startDate[-2:])
    end = int(endDate[-2:])
    
    print("---------Reading data from source--------------")
    
    for i in range(start, end + 1):
        if i < 10:
            days = '0' + str(i) + '.json'
        else:
            days = str(i) + '.json'
            
        path = PATH + days
        date = path.split('\\')[-1].split('.')[0]
        f_date = date[0:4] + "-" + date[4:6] + "-" + date[6:]
        df1 = read_data(path, f_date)
        
        if i > 1:
            df = df.union(df1)
        else:
            df = df1
            
    print("---------Processing data--------------")
    df = process_data(df)
    print("---------Categorize data--------------")
    df = process_category(df)
    print("---------Pivoting Data--------------")
    df = pivot_data(df)
    print("---------Printing output--------------")
    df.show()
    print("---------Saving output--------------")
    df.repartition(1).write.csv(PATH[0:-6] + 'Method1_ETL_30days', mode="overwrite", header = True)
    return print("Task Finished")

if __name__ == "__main__":
    main()