from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_title: str = 'Test clickhouse'
    app_description: str = 'for .repair'
    database_url: str = 'clickhouse+asynch://username:password@localhost/product'
    token: str = '9876543210'
    db_host: str = 'backend'
    db_host_local: str = 'localhost'
    db_port: str = '9000'
    db_user: str = 'username'
    db_password: str = 'password'
    # first_superuser_email: Optional[EmailStr] = None
    # first_superuser_password: Optional[str] = None

    class Config:
        env_file = '.env'


settings = Settings()
