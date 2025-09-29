from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def product_category_pairs(
    products_df: DataFrame,
    categories_df: DataFrame,
    product_categories_df: DataFrame
) -> DataFrame:
    """
    Возвращает датафрейм с парами "Имя продукта - Имя категории" и продуктами без категорий.

    :param products_df: DataFrame с продуктами (столбцы: id, product_name)
    :param categories_df: DataFrame с категориями (столбцы: id, category_name)
    :param product_categories_df: DataFrame связей (столбцы: product_id, category_id)
    :return: DataFrame с колонками product_name, category_name (None если категория отсутствует)
    """
    # Левый джоин продуктов со связями, чтобы сохранить все продукты
    joined = products_df.alias("p").join(
        product_categories_df.alias("pc"),
        col("p.id") == col("pc.product_id"),
        "left"
    ).join(
        categories_df.alias("c"),
        col("pc.category_id") == col("c.id"),
        "left"
    ).select(
        col("p.product_name"),
        col("c.category_name")
    ).orderBy(col("p.product_name"))
    return joined
