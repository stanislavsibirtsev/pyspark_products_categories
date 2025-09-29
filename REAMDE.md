# PySpark Products-Categories Project

## Описание
Этот проект реализует метод на PySpark, который по датафреймам продуктов, категорий и их связей возвращает:
- Все пары "Имя продукта — Имя категории"
- Имена продуктов без категорий (с пустым значением категории)
Метод обеспечивает объединение данных с использованием join, сохраняя все продукты вне зависимости от наличия категорий.

## Структура проекта
```
pyspark_products_categories/
│
├── src/
│   ├── init.py
│   ├── product_category.py # Логика обработки датафреймов
│
├── tests/
│   ├── init.py
│   ├── test_product_category.py # Юнит-тесты
│
├── README.md # Документация проекта
```

## Использование

Импортируйте функцию и передавайте три датафрейма:
```python
from src.product_category import product_category_pairs

result_df = product_category_pairs(products_df, categories_df, product_categories_df)
result_df.show()
```

## Запуск тестов

Из корня проекта выполните:
```bash
python -m unittest discover -s tests
```

## Требования
- Python 3.8+
- PySpark

## Контакты
По вопросам и предложениям открывайте issues или пишите на stansibirtsev@yandex.ru









