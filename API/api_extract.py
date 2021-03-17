import requests
from saml2.authn_context import UNSPECIFIED
from saml2.saml import NameID, NAMEID_FORMAT_UNSPECIFIED
from saml2 import server
import json
import time
import pandas as pd
import asyncio
from aiohttp import ClientSession
import configparser
import os, sys
from db_core import DBConnection, getDatabaseConfig
from config_utils import ConnectionConfig
from db_helpers import getDataframeFromSql
import itertools
import boto3

START_TIME = time.time()
CONCURRENCY = 10
BASE_URL = "https://uat.envestnet.com/"
DEFAULT_HEADERS = {
    "Host": "uat.envestnet.com",
    "Connection": "keep-alive",
    "Expect": "100-continue",
    "X-Channel": "Aspen",
}


def setupDbConfig():
    dbConfig = getDatabaseConfig(None)
    connConfig = ConnectionConfig(
        name="Amazon_Redshift",
        dbtype="redshift",
        library="psycopg2",
        host="fngn-dataeng-edw.c8036hnvermz.us-west-1.redshift.amazonaws.com",
        database="analytics",
        port=5439,
        username="tableaudeuser",
        password="xxxxxx",
    )
    dbConfig.addConnectionConfig(connConfig)


def getAnlyEnvDf():
    setupDbConfig()
    sql = """
    Select distinct 
            ACCTS.customer_id as client_id, 
            ACCTS.account_id, 
            ACCTS.start_date, 
            case when Dates.startDateNum = 20201231 then 20200331
            else GREATEST(TO_NUMBER(TO_CHAR( ACCTS.start_date,'YYYYMMDD'), '99999999'), DATES.startDateNum) 
            end
            as start_date_num,
            LEAST(DATES.endDateNum, 20200630) as end_date_num
        from fe.efs_envest_account_master_snapshot ACCTS
        join (select 
            TO_NUMBER(19001231 + 10000*ROW_NUMBER() over(order by datenum), '99999999') as startDateNum, 
            TO_NUMBER(19011231 + 10000*ROW_NUMBER() over(order by datenum), '99999999') as endDateNum
            from fe.DIM_DATE limit 120) DATES
        on TO_NUMBER(TO_CHAR( ACCTS.start_date,'YYYYMMDD'), '99999999')  < DATES.endDateNum 
        where ACCTS.account_status <> 3 
        and ACCTS.datenum = (select max(datenum) from 
        fe.efs_envest_account_master_snapshot) 
        order by client_id, ACCTS.account_id, start_date_num, end_date_num asc limit 100
    """
    clientIdDf = getDataframeFromSql("Amazon_Redshift", sql)
    return clientIdDf


def getCompletedCalls():
    clientCompDf = pd.DataFrame(
        columns=["client_id", "start_date_num", "end_date_num"]
    )
    if len(clientCompDf) == 0:
        clientCompDf = pd.DataFrame(
            columns=["client_id", "start_date_num", "end_date_num", "done"]
        )
        acctCompDf = pd.DataFrame(
            columns=[
                "client_id",
                "account_id",
                "start_date_num",
                "end_date_num",
                "done",
            ]
        )
    return clientCompDf, acctCompDf


def webServiceCall(url, headers, data=None):
    with requests.Session() as session:
        req = None
        if data is None:
            req = requests.Request("GET", url, headers=headers)
        else:
            req = requests.Request("POST", url, data=data, headers=headers)
        prep = req.prepare()
        resp = session.send(prep)
    return resp


def get_auth_token():
    samlServer = server.Server("idp_conf")
    nid = NameID(format=NAMEID_FORMAT_UNSPECIFIED, text="ganderson")
    authn_dict = {
        "decl": "authn_decl",
        "class_ref": UNSPECIFIED,
        "authn_instant": time.time(),
    }
    samlResp = samlServer.create_authn_response(
        identity={},
        in_response_to="",
        userid="ganderson",
        destination="https://uat.envestnet.com/openenv/api/auth/login?firm=edelman",
        sp_entity_id=None,
        sign_assertion=True,
        name_id=nid,
        authn=authn_dict,
    )

    url = "https://uat.envestnet.com/openenv/api/auth/login?firm=edelman"

    headers = DEFAULT_HEADERS.copy()
    headers["Content-Type"] = "application/xml; charset=utf-8"

    resp = webServiceCall(url, headers, data=samlResp)
    return resp.headers["Token"]


# get a list of users
REFRESH_TIME = time.time()
AUTH_TOKEN = get_auth_token()


def run(items):
    loop = getEventLoop()
    results = None
    try:
        results = loop.run_until_complete(tasks(items))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
    return results


sem = None
lock = None
cond = None


def getEventLoop():
    global sem
    global lock
    global cond
    loop = asyncio.get_event_loop()
    if loop._closed:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    sem = asyncio.Semaphore(CONCURRENCY, loop=loop)
    lock = asyncio.Lock(loop=loop)
    cond = asyncio.Condition(lock=lock, loop=loop)
    return loop


async def tasks(items):
    tasks = [
        asyncio.ensure_future(concurrentWebServiceCall(*item)) for item in items
    ]
    return await asyncio.gather(*tasks)  #


async def concurrentWebServiceCall(url, collName, filename, addlDict={}):
    async with sem:  # semaphore limits num of simultaneous calls
        if (time.time() - REFRESH_TIME) > 600:
            async with cond:
                print(
                    f"Time since last Token Refresh {time.time() - REFRESH_TIME}"
                )
                refreshAuthToken()
        return await asyncWebServiceCall(url, collName, filename, addlDict)


async def asyncWebServiceCall(url, collName, filename, addlDict={}):
    headers = DEFAULT_HEADERS.copy()
    headers["Authorization"] = f"Bearer {AUTH_TOKEN}"

    # acctURL = f"https://uat.envestnet.com/openenv/api/performance/{account_handle}?firm=edelman"
    async with ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            output = await response.read()

    date = response.headers.get("DATE")
    print("{}: got response for {}".format(date, url))
    outputJson = json.loads(output)
    # print(outputJson)
    # print(df.head())

    s3 = boto3.client('s3')
    bucket = "com-fngn-prod-datalake-raw"

    if isinstance(outputJson, list):
        for item in outputJson:
            item.update(addlDict)

    else:
        outputJson.update(addlDict)
    print(addlDict)
    s3.upload_file(filename, bucket, f"irr/{filename}")
    # with open(filename, 'w') as outfile:
    #    json.dump(outputJson, outfile)
    if "errors" in outputJson.keys():
        return False
    return True


def runAPICalls():
    clientCompDf, acctCompDf = getCompletedCalls()

    clientDf = getAnlyEnvDf()
    print(clientDf.count())

    clientDf = clientDf.merge(
        acctCompDf,
        how="left",
        on=["client_id", "account_id", "start_date_num", "end_date_num"],
    )

    print(clientDf[clientDf.done.isnull()].count())

    clientAPIDf = clientDf[
        ["client_id", "start_date_num", "end_date_num"]
    ].drop_duplicates()

    print(clientAPIDf.count())
    clientAPIDf = clientAPIDf.merge(
        clientCompDf, how="left", on=["client_id", "start_date_num", "end_date_num"]
    )
    print(clientAPIDf[clientAPIDf.done.isnull()].count())

    viewUrl = f"{BASE_URL}openenv/api/reports/views/{{}}/performance?periodStartDate={{}}&periodEndDate={{}}"

    acctViewGenerator = (
        [
            viewUrl.format(
                f"2~{x.client_id}~2~{x.account_id}",
                int(x.start_date_num),
                int(x.end_date_num),
            ),
            "acctPerformance",
            f"acctPerformance_{x.account_id}_{x.start_date_num}_{x.end_date_num}.json",
            {
                "client_id": x.client_id,
                "account_id": x.account_id,
                "start_date_num": int(x.start_date_num),
                "end_date_num": int(x.end_date_num),
            },

        ]
        for i, x in clientDf[clientDf.done.isnull()].iterrows()
    )

    clientViewGenerator = (
        [
            viewUrl.format(
                f"2~{x.client_id}~0~{x.client_id}", int(x.start_date_num), int(x.end_date_num)
            ),
            "clientPerformance",
            f"clientPerformance_{x.client_id}_{x.start_date_num}_{x.end_date_num}.json",
            {
                "client_id": x.client_id,
                "start_date_num": int(x.start_date_num),
                "end_date_num": int(x.end_date_num),
            },

        ]
        for i, x in clientAPIDf[clientAPIDf.done.isnull()].iterrows()
    )

    def chunkIterator(iterator, n):
        while True:
            yield itertools.islice(
                iterator, 0, n
            )  # yield `n` size chunk of the iterator

    itemsIter = itertools.chain(acctViewGenerator, clientViewGenerator)

    REFRESH_TIME = time.time()
    AUTH_TOKEN = get_auth_token()

    for chunkIter in chunkIterator(itemsIter, 50000):
        items = list(chunkIter)
        if len(items) == 0:
            break
        outputs = run(items)

    totalTime = time.time() - START_TIME
    totalCalls = clientAPIDf["client_id"].count() + clientDf["client_id"].count()
    print(f"Total Calls: {totalCalls}")
    print(f"Total Time: {totalTime}")
    print(f"Num calls Per Sec:  {totalCalls / totalTime}")
    print(f"Time Per call:  {totalTime * CONCURRENCY / totalCalls}")

if __name__ == "__main__":
    runAPICalls()
