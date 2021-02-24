#************************************************************
# Script Name: fileWatcher.py
# Developer:   Ram Modepalli
# Purpose: To check the input file availability
#          If File exists exit the program with Return code 0 
#          If not exists then keep on Checking for 60 min
#          and exit when file arrived. If file not arrived in
#          in 60 min exit with error code
# Revision History:
# Name                   ReasonForChange          ModifiedDate
# ------------------------------------------------------------ 
# Ram Modepalli          Initial Version         2017-08-28
#*************************************************************
import os
import argparse
import sys
import time
from datetime import datetime
from datetime import timedelta
import boto3
import fnmatch

if __name__ == '__main__':

    default_wait_time = 3600
    default_sleep_time = 120

    # Parse Command Line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--filename', nargs=1, help='FileName With Absolute Path', required=True, type=str)
    parser.add_argument('--waittime', nargs=1, help='Wait time in mins before exiting with error', required=False, type=int)
    parser.add_argument('--awsprofile', nargs=1, help='AWS ProfileName', required=False, type=str)
    try:
        args = parser.parse_args()
    except argparse.ArgumentError:
        sys.exit(1)

    # Check the arguments

    filename = args.filename[0]
    seconds = 60
    if args.waittime is not None:
        wait_time = args.waittime[0] * seconds
    else:
        wait_time = default_wait_time

    # Assign the Year, Month, Dare variables
    current_day = datetime.now()
    prev_day = datetime.now() - timedelta(days=1)

    todate = datetime.strftime(current_day, '%Y-%m-%d')
    todatenum = datetime.strftime(current_day, '%Y%m%d')

    yesterdate = datetime.strftime(prev_day, '%Y-%m-%d')
    yesterdatenum = datetime.strftime(prev_day, '%Y%m%d')

    # Replace the variables in FileName

    filename = filename.replace('$TODATENUM', str(todatenum))
    filename = filename.replace('$TODATE', str(todate))
    filename = filename.replace('$YESTERDATENUM', str(yesterdatenum))
    filename = filename.replace('$YESTERDATE', str(yesterdate))
    
    # If AWS profile passed as parameter then use it else assume the EC2 role
    if args.awsprofile is None:
        s3client = boto3.client('s3')
    else:
        session = boto3.Session(profile_name=args.awsprofile[0])
        s3client = session.client('s3')
    
    while True:
        # Check If File exists

        # If file is in S3 . Filename starts with s3://<BucketName>/FilePath

        if filename.startswith('s3://'):
            # Parse bucket Name and FilePath
            s3bucket = filename.replace('s3://', '').split('/')[0]
            s3Key = filename.replace('s3://', '').replace(s3bucket + '/', '')
            results = s3client.list_objects(Bucket=s3bucket, Prefix=s3Key)        
            if 'Contents' in results:
                print("File {} arrived. Exiting...".format(filename))
                sys.exit(0)
        elif len(fnmatch.filter(os.listdir(os.path.dirname(filename)), os.path.basename(filename))) > 0:
            print("File {} arrived. Exiting..".format(filename))
            sys.exit(0)
        else:
            # Sleep for 2 min
            time.sleep(default_sleep_time)

        # Check If wait time exceeds expected wait time
        delta = datetime.now() - current_day
   
        if delta.seconds > wait_time:
            print("File {} not arrived in {} min(s). Exiting".format(filename, int(wait_time / seconds)))
            sys.exit(1)
