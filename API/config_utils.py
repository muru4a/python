import os
import yaml
from typing import List, Iterator, Optional
from dataclasses import dataclass, field, InitVar
import itertools
from collections import namedtuple

ConnAttr = namedtuple("ConnAttr", ["required", "optional"])


def here():
    return os.path.dirname(os.path.abspath(__file__))


def getConfigFile(filename: Optional[str]) -> Optional[str]:
    if filename is None:
        return None
    configDir = os.path.join(here(), "..", "config")
    configFileName = os.path.join(configDir, filename)
    return configFileName


def loadYamlFile(filename: Optional[str]):
    if filename is None:
        return {"Connections": {}}
    with open(filename) as cf:
        yamlDoc = yaml.load(cf, Loader=yaml.FullLoader)
    return yamlDoc


def writeYamlDocToFile(yamlDoc, filename: str) -> None:
    with open(filename, "wb") as cf:
        cf.write(yaml.dump(yamlDoc))


def getConfigCombinations(hasDsnAttr=False) -> Iterator:
    allConfigList = [
        ["nt", "posix"],
        ["mssql", "redshift"],
        ["pyodbc", "psycopg2"],
    ]
    if hasDsnAttr:
        allConfigList = [["nt", "posix"], ["mssql", "redshift"], ["pyodbc"]]
    configIter = itertools.product(*allConfigList)

    def func(x):
        return ("mssql" in x) and ("psycopg2" in x)

    filteredIter = itertools.filterfalse(func, configIter)
    return filteredIter


def getRequiredAttrList(hasDsnAttr=False) -> ConnAttr:
    attrsDsn = ConnAttr(
        required=[
            "dbtype",
            "library",
            "database",
            "dsn",
            "username",
            "password",
        ],
        optional=[],
    )
    if os.name == "nt":
        attrsDsn = ConnAttr(
            required=["dbtype", "library", "database", "dsn"],
            optional=["username", "password"],
        )
    attrsHost = ConnAttr(
        required=[
            "dbtype",
            "library",
            "database",
            "host",
            "port",
            "username",
            "password",
        ],
        optional=[],
    )
    return attrsDsn if hasDsnAttr else attrsHost


@dataclass
class ConnectionConfig:
    name: str
    dbtype: str
    library: str
    database: str
    driver: str = None
    dsn: str = None
    host: str = None
    port: str = None
    username: str = None
    password: str = None

    def __post_init(self):
        # Check required attributes are present
        hasDsnAttr = self.dsn is not None
        attrs = getRequiredAttrList(hasDsnAttr)
        for item in attrs.required:
            eMsg = f"Required attribute {item} missing for config {self.name}"
            assert getattr(self, item, None) is not None, eMsg

        # Check if it is a valid combination
        configIter = getConfigCombinations(hasDsnAttr)
        combo = [os.name, self.dbtype, self.library]
        matched = False
        while not matched:
            if combo == next(configIter):
                matched = True
                break
        assert (
            matched
        ), f"Unsupported combination {combo} for config {self.name}"

    def getConfigDict(self) -> dict:
        configDict = {}
        for fld, fType in self.__annotations__.items():
            value = getattr(self, fld, None)
            if value is not None:
                configDict[fld] = value
        return configDict


class DatabaseConfig:
    _shared_state: dict = {}

    def __init__(self, config_file: Optional[str] = None):
        """
        need to check if config_file exists
        """
        self.__dict__ = self._shared_state
        if not hasattr(self, "configFile"):
            self.loadConfig(config_file)

    def loadConfig(self, config_file: Optional[str]):
        self.configFile: Optional[str] = getConfigFile(config_file)
        self.config = loadYamlFile(self.configFile)

    def reloadConfig(self):
        self.loadConfig(self.configFile)

    def getAllConnectionProfiles(self):
        return self.config["Connections"].keys()

    def addConnectionConfig(self, connConfig: ConnectionConfig):
        configDict = connConfig.getConfigDict()
        name = configDict.pop("name")
        self.config["Connections"][name] = configDict

    def getConnectionByName(self, name: str) -> ConnectionConfig:
        errorMsg = "Unable to find connection details for "
        errorMsg += f"{name} in config file {self.configFile}"
        assert name in self.config["Connections"].keys(), errorMsg
        return ConnectionConfig(name=name, **self.config["Connections"][name])
