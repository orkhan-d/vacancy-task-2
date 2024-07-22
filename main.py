from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

products_df = spark.createDataFrame([
    (1, "Product 1"),
    (2, "Product 2"),
    (3, "Product 3"),
    (4, "Product 4"),
    (5, "Product 5"),
    (6, "Product 6"),
    (7, "Product 7"),
    (8, "Product 8"),
    (9, "Product 9"),
    (10, "Product 10")
], ["product_id", "product_name"])

categories_df = spark.createDataFrame([
    (1, "Category 1"),
    (2, "Category 2"),
    (3, "Category 3")
], ["category_id", "category_name"])

product_category_df = spark.createDataFrame([
    (1, 1),
    (2, 1),
    (2, 2),
    (5, 2),
    (5, 3),
    (6, 3),
    (8, 1),
    (8, 2),
    (9, 3),
    (10, 1)
], ["product_id", "category_id"])


def get_all_product_category_info():
    products_pairs_df = (product_category_df
                         .join(products_df, "product_id")
                         .join(categories_df, "category_id")
                         .select("product_name", "category_name"))
    products_without_pairs_df = (products_df
                                 .join(product_category_df, "product_id", "left_anti")
                                 .select("product_name"))

    return products_pairs_df.unionByName(products_without_pairs_df, allowMissingColumns=True)


get_all_product_category_info().show()
