import pyspark.sql.types as T

ingestionSchema = T.StructType([
    T.StructField('RowID', T.StringType(), False),
    T.StructField('OrderID', T.StringType(), False),
    T.StructField('OrderDate', T.DateType(), False),
    T.StructField('ShipDate', T.DateType(), True),
    T.StructField('ShipMode', T.StringType(), True),
    T.StructField('CustomerID', T.StringType(), False),
    T.StructField('CustomerName', T.StringType(), True),
    T.StructField('Segment', T.StringType(), True),
    T.StructField('Country', T.StringType(), True),
    T.StructField('City', T.StringType(), True),
    T.StructField('State', T.StringType(), True),
    T.StructField('PostalCode', T.StringType(), True),
    T.StructField('Region', T.StringType(), True),
    T.StructField('ProductId', T.StringType(), False),
    T.StructField('Category', T.StringType(), True),
    T.StructField('SubCategory', T.StringType(), True),
    T.StructField('ProductName', T.StringType(), True),
    T.StructField('Sales', T.DoubleType(), True)
])

selectedFields = [
    'RowID',
    'OrderID',
    'OrderDate',
    'ShipDate',
    'ShipMode',
    'CustomerID',
    'CustomerName',
    'Segment',
    'Country',
    'City'
]
