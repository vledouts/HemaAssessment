import pyspark.sql.types as T

ingestionSchema = T.StructType([
    T.StructField('rowID', T.StringType(), False),
    T.StructField('orderID', T.StringType(), False),
    T.StructField('orderDate', T.DateType(), False),
    T.StructField('shipDate', T.DateType(), True),
    T.StructField('shipMode', T.StringType(), True),
    T.StructField('customerID', T.StringType(), False),
    T.StructField('customerName', T.StringType(), True),
    T.StructField('segment', T.StringType(), True),
    T.StructField('country', T.StringType(), True),
    T.StructField('city', T.StringType(), True),
    T.StructField('state', T.StringType(), True),
    T.StructField('postalCode', T.StringType(), True),
    T.StructField('region', T.StringType(), True),
    T.StructField('productId', T.StringType(), False),
    T.StructField('category', T.StringType(), True),
    T.StructField('subCategory', T.StringType(), True),
    T.StructField('productName', T.StringType(), True),
    T.StructField('sales', T.DoubleType(), True)
])

selectedFields = [
    'rowID',
    'orderID',
    'orderDate',
    'shipDate',
    'shipMode',
    'customerID',
    'customerName',
    'segment',
    'country',
    'city'
]

customerKeys = [
    'customerId',
    'customerName',
    'customerFirstName',
    'customerLastName',
    'customerSegment',
    'country',
    'city'
]
