import unittest
import psycopg2
import psycopg2.extras
import sys
import os
import configparser
import argparse

class DataValidation(unittest.TestCase):

    def __init__(self, test_sk, testCaseName, RSCursor, sql, expectedValue, compareOperator='='):
        super().__init__()
        self.test_sk = test_sk
        self.testCaseName = testCaseName
        self.RSCursor = RSCursor
        self.sql = sql
        self.expectedValue = expectedValue
        self.compareOperator = compareOperator

    def executeSQL(self):
        try:
            self.RSCursor.execute(self.sql)
        except psycopg2.Error as error:
            print(error.args[0])
            sys.exit(-1)

        #Create dictionary form Loader metadata
        meta_column_names = [col[0] for col in self.RSCursor.description]

        if len(meta_column_names) != 1 or self.RSCursor.rowcount != 1:
            print("Invalid test case. SQL should retrun one value")
            sys.exit(-1)

        retval = self.RSCursor.fetchone()

        return str(retval[meta_column_names[0]])

    def runTest(self):
        print("testCaseName: " + self.testCaseName)

        #Execute the SQL and fetch the scalar value returned by SQL
        actualvalue = self.executeSQL()

        if self.compareOperator not in ('=', '!=', '<', '<=', '>', '>='):
            print("Invalid comparision operator")
            sys.exit(-1)

        insert_stmt = '''
            INSERT INTO test_run_log (test_sk, is_test_success, observed_value) VALUES 
            ({}, {}, {})
        '''

        if self.compareOperator == '=':
            is_test_success = (actualvalue == self.expectedValue)
            insert_stmt = insert_stmt.format(self.test_sk, is_test_success, actualvalue)
            self.RSCursor.execute(insert_stmt)
            self.assertEqual(actualvalue, self.expectedValue)
        elif self.compareOperator == '!=':
            is_test_success = (actualvalue != self.expectedValue)
            insert_stmt = insert_stmt.format(self.test_sk, is_test_success, actualvalue)
            self.RSCursor.execute(insert_stmt)
            self.assertNotEqual(actualvalue, self.expectedValue)
        elif self.compareOperator == '<':
            is_test_success = (actualvalue < self.expectedValue)
            insert_stmt = insert_stmt.format(self.test_sk, is_test_success, actualvalue)
            self.RSCursor.execute(insert_stmt)
            self.assertLess(actualvalue, self.expectedValue)
        elif self.compareOperator == '<=':
            is_test_success = (actualvalue <= self.expectedValue)
            insert_stmt = insert_stmt.format(self.test_sk, is_test_success, actualvalue)
            self.RSCursor.execute(insert_stmt)
            self.assertLessEqual(actualvalue, self.expectedValue)
        elif self.compareOperator == '>':
            is_test_success = (actualvalue > self.expectedValue)
            insert_stmt = insert_stmt.format(self.test_sk, is_test_success, actualvalue)
            self.RSCursor.execute(insert_stmt)
            self.assertGreater(actualvalue, self.expectedValue)
        elif self.compareOperator == '>=':
            is_test_success = (actualvalue >= self.expectedValue)
            insert_stmt = insert_stmt.format(self.test_sk, is_test_success, actualvalue)
            self.RSCursor.execute(insert_stmt)
            self.assertGreaterEqual(actualvalue, self.expectedValue)
        

if __name__ == '__main__':

    # Parse Command Line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--testsuite_testcase', nargs=1, help='Test Suite Name or Test Name', required=True, type=str)
    
    try:
        args = parser.parse_args()
    except:
        sys.exit(1)
        
    testsuite_testcase = args.testsuite_testcase[0]
    
    # *****************************************************
    # Load the etl config File
    # *****************************************************
    cfg = configparser.ConfigParser()
    # if ETL_CFG_FILE env variable defined then use that file
    # else use default location /mnt/etl/config/etl.cfg
    if "ETL_CFG_FILE" in os.environ:
        if os.path.exists(os.environ['ETL_CFG_FILE']):
            etl_cfg_file = os.environ['ETL_CFG_FILE']
        else:
            print("Config File {} doesn't exist".format(os.environ['ETL_CFG_FILE']))
            sys.exit(1)
    else:
        if os.path.exists('/mnt/etl/config/etl.cfg'):
            etl_cfg_file = '/mnt/etl/config/etl.cfg'
        else:
            print("Config File /mnt/etl/config/etl.cfg doesn't exist")
            sys.exit(1)

    cfg.read(etl_cfg_file)
    
    rs_host = cfg['Redshift']['RS_HOST']
    rs_user = cfg['Redshift']['RS_USER']
    rs_password = cfg['Redshift']['RS_PASSWORD']
    rs_database = cfg['Redshift']['RS_DATABASE']

    try:
        rscon = psycopg2.connect(host=rs_host, user=rs_user, password=rs_password, dbname=rs_database, port='5439')
    except psycopg2.Error as error:
        print(error.args[0])
        sys.exit(-1)

    print("Connected to Redshift Database {} with user {} on Host {}".format(rs_database, rs_user, rs_host))

    rscursor = rscon.cursor(cursor_factory=psycopg2.extras.DictCursor)
    setpathsql = 'SET SEARCH_PATH TO semantic, core, stage, ops, reporting'
    try:
        rscursor.execute(setpathsql)
    except psycopg2.Error as error:
        print("Error during execution of SQL: {} ".format(setpathsql))
        print(error.args[0])
        sys.exit(-1)

    # Read the Test Suite or TestCase metadata from test_catalog
    sql = '''
            SELECT 
                   *
            FROM
                test_catalog
            WHERE
                (test_suite = '{}'
                OR test_name = '{}')
                AND is_test_enabled                
          '''
    sql = sql.format(testsuite_testcase, testsuite_testcase)
    
    try:
        rscursor.execute(sql)
    except psycopg2.Error as error:
        print("Error during execution of SQL: {} ".format(sql))
        print(error.args[0])
        sys.exit(-1)
      
    suite = unittest.TestSuite()

    for testcase in rscursor.fetchall():
        test_sk = testcase['test_sk']
        testname = testcase['test_name']
        testSql = testcase['observed_sql']
        expected_value = testcase['expected_value']
        assertion_expr = testcase['assertion_expr']
        
        # Add the testcase
        suite.addTest(DataValidation(test_sk, testname, rscursor, testSql, expected_value, assertion_expr))
        
    ret = unittest.TextTestRunner().run(suite).wasSuccessful()
    rscon.commit()
    rscon.close()
    
    if ret:
        sys.exit(0)
    else:
        sys.exit(1)


