### Описание

Схема "Звезда".PostgreSQL.
Все скрипты в папке sql_scripts. Проверены тестами

### Инструменты

- docker
- docker-compose
- uv

### Запуск

1. Клонировать репозиторий и перейти в директорию проекта:

   ```bash
   git clone ...
   cd star_schema_olap
   ```

2. Создать .env файл с переменными окружения:

   ```bash
   cp .env.example .env
   ```

3. Запустить docker-compose для запуска базы данных:

   ```bash
   docker compose up -d
   ```

4. Запуск тестов:
   ```bash
   uv run pytest
   ```
