import os

DB_CONFIG = {
    "DATABASE": os.getenv("DB_NAME"),
    "USER": os.getenv("DB_USERNAME"),
    "PASSWORD": os.getenv("DB_PWD"),
    "HOST": os.getenv("DB_HOST"),
    "PORT": os.getenv("DB_PORT")
}

API_KEY_FINN = {
    "API_KEY_FINN": os.getenv("API_KEY_FINN")
}


print(DB_CONFIG)
print(API_KEY_FINN)
