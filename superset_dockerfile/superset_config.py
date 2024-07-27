# import os
# from datetime import timedelta

# # Основные настройки
# SECRET_KEY = os.getenv('SUPERSET_SECRET_KEY', 'thisISaSECRET_1234')
# SQLALCHEMY_DATABASE_URI = 'sqlite:////app/superset_home/superset.db'

# # Настройки безопасности
# CSRF_ENABLED = True
# WTF_CSRF_ENABLED = True
# WTF_CSRF_TIME_LIMIT = None  # или установите конкретное значение в секундах, например 3600

# # Отключение аутентификации
# AUTH_TYPE = 3  # Использование AUTH_OID (OpenID Connect) для отключения стандартной аутентификации
# AUTH_USER_REGISTRATION = True
# AUTH_USER_REGISTRATION_ROLE = "Admin"

# # Режим разработки
# DEBUG = True
# FLASK_ENV = "development"


import os
from datetime import timedelta
from superset.config import *

# # Отключение аутентификации
# AUTH_TYPE = AUTH_REMOTE_USER
# AUTH_USER_REGISTRATION = True
# AUTH_USER_REGISTRATION_ROLE = "Admin"

# # Режим разработки
# DEBUG = True
# FLASK_ENV = "development"

# Основные настройки
SECRET_KEY = os.getenv('SUPERSET_SECRET_KEY', 'thisISaSECRET_1234')
SQLALCHEMY_DATABASE_URI = 'sqlite:////app/superset_home/superset.db'

# Настройки безопасности
CSRF_ENABLED = True
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None  # или установите конкретное значение в секундах, например 3600

# Дополнительные настройки могут быть добавлены здесь