# Задание
[Тестовое задание](https://vk-intership-data-engineering.netlify.app/)

# Реализован минимальный и рекомендуемый функционал
## Минимальный функционал
Просто запускаем из текущей директории репозитория  
```
python3 main.py <YYYY-mm-dd>
```
## Рекомендуемый функционал
- Можно перейти в папку `/scripts` и вручную запустить скрипт `spark_app.py` приложения  
```
python3 spark_app.py <YYYY-mm-dd>
```
- А можно запустить Apach Airflow с помощью `docker compose` файла:  
Для запуска моего Spark App клонируем и используем -  
```
docker compose build #сначала собираем наш образ  
docker compose up -d #а затем его запускаем
```  
Так же не забываем менять `FERNET_KEY` на свой в docker-compose.yml.

![image](https://github.com/user-attachments/assets/b202f040-32ed-4f37-8052-db9360ec6426)
