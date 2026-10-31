import questionary
from datetime import datetime
from dotenv import load_dotenv
import os

def prompt_year() -> str:
    current = datetime.now().year
    choices = [str(current + i) for i in range(-4, 1)]
    return questionary.select("Elija un año a evaluar:", choices=choices).ask()

def setup_environment_variables(env_flag: str) -> tuple[str, str]:
    load_dotenv()
    main_path = ""
    migration_path = ""
    if env_flag == "p":
        main_path = os.getenv("PRODUCTION_PATH")
        migration_path = os.getenv("DB_PRODUCTION_PATH")
    elif env_flag == "t":
        main_path = os.getenv("TESTING_PATH")
        migration_path = os.getenv("DB_TESTING_PATH")
    if main_path is None:
        raise RuntimeError("The environment variable 'PRODUCTION_PATH' or 'TESTING_PATH' is not set.")
    if migration_path is None:
        raise RuntimeError("The environment variable 'DB_PRODUCTION_PATH' or 'DB_TESTING_PATH' is not set.")
    return main_path, migration_path