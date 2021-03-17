from config_utils import DatabaseConfig
import sqlalchemy as sa
import urllib
from typing import Union


def getDatabaseConfig(configFile: str = "config.yml") -> DatabaseConfig:
    databaseConfig = DatabaseConfig(configFile)
    return databaseConfig


class BaseDBConnection:
    def __init__(self, configDict):
        self.attrs = []
        self.attrMap = {}
        self.skippedAttrs = ["name", "library", "dbtype"]
        self.attrs.extend(configDict.keys())

        for key, value in configDict.items():
            self.__dict__[key] = value
        self.lib = __import__(self.library)

    def getConnectionString(self):
        pass

    def getEngineString(self):
        pass

    def getEngine(self):
        try:
            return sa.create_engine(self.getEngineString())
        except sa.exc.NoSuchModuleError:
            return None

    def getConnection(self):
        conn_str = self.getConnectionString()
        return self.lib.connect(conn_str)

    def __repr__(self):
        return self.getConnectionString()


class PyodbcDatabaseConnection(BaseDBConnection):
    def __init__(self, configDict):
        super().__init__(configDict)
        self.attrMap = {
            "driver": "DRIVER",
            "dsn": "DSN",
            "host": "SERVER",
            "port": "PORT",
            "username": "UID",
            "password": "PWD",
            "database": "DATABASE",
        }
        if getattr(self, "dsn", False):
            self.skippedAttrs.append("driver")

    def getConnectionString(self):
        cStr = ""
        for item in self.attrs:
            if item in self.skippedAttrs:
                continue
            value = getattr(self, item, "")
            if value:
                cStr += f"{self.attrMap.get(item, item)}={value};"
        return cStr

    def getEngineString(self):
        params = urllib.parse.quote_plus(self.getConnectionString())
        return f"{self.dbtype}+{self.library}:///?odbc_connect={params}"


class Psycopg2DatabaseConnection(BaseDBConnection):
    def getConnectionString(self):
        cStr = f"dbname={self.database} "
        cStr += f"host={self.host} "
        cStr += f"port={self.port} "
        cStr += f"user={self.username} "
        cStr += f"password={self.password} "
        return cStr

    def getEngineString(self):
        return sa.engine.url.URL(
            f"{self.dbtype}+{self.library}",
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.username,
            password=self.password,
        )


class DBConnection:
    handle: Union[Psycopg2DatabaseConnection, PyodbcDatabaseConnection]

    def __init__(self, conn_name: str):
        connConfig = getDatabaseConfig().getConnectionByName(conn_name)
        configDict = connConfig.getConfigDict()
        if connConfig.library == "psycopg2":
            self.handle = Psycopg2DatabaseConnection(configDict)
        elif connConfig.library == "pyodbc":
            self.handle = PyodbcDatabaseConnection(configDict)

    def getConnectionString(self):
        return self.handle.getConnectionString()

    def getEngineString(self):
        return self.handle.getEngineString()

    def getEngine(self):
        return self.handle.getEngine()

    def getConnection(self):
        return self.handle.getConnection()

    def __repr__(self):
        return self.handle.getConnectionString()
