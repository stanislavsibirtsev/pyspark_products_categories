import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.product_category import product_category_pairs

class TestProductCategoryPairs(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_product_category_pairs(self):
        products_data = [(1, "Apple"), (2, "Banana"), (3, "Cherry")]
        categories_data = [(10, "Fruit"), (20, "Food")]
        product_categories_data = [(1, 10), (2, 10)]

        products_df = self.spark.createDataFrame(products_data, ["id", "product_name"])
        categories_df = self.spark.createDataFrame(categories_data, ["id", "category_name"])
        product_categories_df = self.spark.createDataFrame(product_categories_data, ["product_id", "category_id"])

        result_df = product_category_pairs(products_df, categories_df, product_categories_df)

        expected = [
            ("Apple", "Fruit"),
            ("Banana", "Fruit"),
            ("Cherry", None)  # Без категории
        ]

        result_list = [(row.product_name, row.category_name) for row in result_df.collect()]

        self.assertEqual(result_list, expected)

if __name__ == "__main__":
    unittest.main()
