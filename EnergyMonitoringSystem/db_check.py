
from backend.dal.database import db_helper
import sys

if db_helper.test_connection():
    print("DB_CONNECTION_SUCCESS")
    sys.exit(0)
else:
    print("DB_CONNECTION_FAILURE")
    sys.exit(1)
