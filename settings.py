from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    SNOWFLAKE_ACCOUNT: str = ""
    SNOWFLAKE_USER: str = ""
    SNOWFLAKE_PASSWORD: str = ""
    SNOWFLAKE_DATABASE: str = None
    SNOWFLAKE_WAREHOUSE: str = None

    class Config:
        env_file = ".env"
        extra = "ignore"

    def get_snowflake_dict(self) -> dict:
        return dict(
            account=self.SNOWFLAKE_ACCOUNT,
            user=self.SNOWFLAKE_USER,
            password=self.SNOWFLAKE_PASSWORD,
            database=self.SNOWFLAKE_DATABASE,
            warehouse=self.SNOWFLAKE_WAREHOUSE,
        )


settings = Settings()
