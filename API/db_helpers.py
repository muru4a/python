from db_core import DBConnection
import pandas as pd
from contextlib import closing


def executeSql(connName, sqlString):
    DBConn = DBConnection(connName)
    with DBConn.getConnection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sqlString)
        conn.commit()
    return


def getDataframeFromSql(connName, sqlString):
    DBConn = DBConnection(connName)
    df = None
    with DBConn.getConnection() as conn:
        df = pd.read_sql(sqlString, conn)
    return df


def writeDataFrameToTable(
    connName,
    inputDf,
    tableName,
    chunksize=1000,
    schema=None,
    truncate_first=False,
):
    """ Appends the dataframe to the table specifed
    """

    if truncate_first:
        truncTable = ".".join([schema, tableName])
        truncateSQL = "TRUNCATE TABLE {0}".format(truncTable)
        executeSql(connName, truncateSQL)

    DBConn = DBConnection(connName)
    engine = DBConn.getEngine()

    if engine is None:
        fullTableName = (
            f"{schema}.{tableName}" if schema is not None else tableName
        )
        with closing(DBConn.getConnection()) as conn:
            with closing(conn.cursor()) as cursor:
                cols = "`,`".join([str(i) for i in inputDf.columns.tolist()])
                dfLen = len(inputDf)
                chunkStarts = [x for x in range(dfLen) if (x % chunksize == 0)]
                chunkEnds = [
                    x for x in range(dfLen) if (x % chunksize == chunksize - 1)
                ]

                for i, row in inputDf.iterrows():
                    values = str(row.tolist())[1:-1]
                    if i in chunkStarts:
                        sql = f"INSERT INTO {fullTableName} ({cols}) VALUES ({values}), \n"
                    elif (i in chunkEnds) or (i == (dfLen - 1)):
                        sql += f"({values});\n"
                        cursor.execute(sql)
                    else:
                        sql += f"({values}), \n"
                conn.commit()
    else:
        with engine.connect() as conn:
            inputDf.to_sql(
                tableName,
                conn,
                schema=schema,
                if_exists="append",
                index=False,
                index_label=None,
                chunksize=chunksize,
                method="multi",
            )
