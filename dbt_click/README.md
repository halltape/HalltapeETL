# Проект DBT: Загрузка данных из Data Lake в ClickHouse (dbt-click)

## Описание

Этот проект DBT предназначен для загрузки и обработки данных из Data Lake в базу данных ClickHouse с использованием моделей `monitoring` и `datamart`. Проект структурирован таким образом, чтобы обеспечить эффективную интеграцию и преобразование данных для аналитических целей, а также для обеспечения контроля качества данных.

## Структура проекта

- **models/**: Содержит модели DBT для обработки и анализа данных.
  - **monitoring/**: Модели для мониторинга данных и контроля качества.
  - **datamart/**: Модели для загрузки и преобразования данных в целевой формат для аналитики.