#!/usr/bin/python
import sys
import json
import argparse
from etl_engine import load_config
from etl_engine import run_etl

if __name__ == '__main__':

    help_test_run = "When testing show timestamp in console log. Allowed values: yes, y; otherwise no"

    # Parse Command Line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--loadercode', nargs=1, help='Loader Code', required=True, type=str)

    # Provide the variables that need to run etl_engine in dictionary format
    parser.add_argument('--etl_var_dict', nargs=1, help='In case of to override the etl config ', required=False,
                        type=str)

    # Provide the cfg in dictionary format .  ex {'S3.S3_DATALAKE_ENRICHED_BUCKET':'com-fngn-prod-dataeng'}
    # Where dict key is cfg category , cfg variable concatinated by . dictionary can have multiple keys
    parser.add_argument('--oride_etl_cfg', nargs=1, help='In case of to override the etl config ', required=False,
                        type=str)
    try:
        args = parser.parse_args()
    except:
        sys.exit(1)

    # Check the arguments

    loadercode = args.loadercode[0]

    if args.etl_var_dict is not None:
        print(args.etl_var_dict[0])
        try:
            etl_var_dict = json.loads(args.etl_var_dict[0])
        except json.decoder.JSONDecodeError:
            print('The etl var info should be in dictionary format')
            sys.exit(-1)
    else:
        etl_var_dict = None

    if args.oride_etl_cfg is not None:
        try:
            oride_etl_cfg = json.loads(args.oride_etl_cfg[0])
        except json.decoder.JSONDecodeError:
            print('The etl config should be in dictionary format')
            sys.exit(-1)

        for cfg_key in oride_etl_cfg.keys():
            if len(cfg_key.split('.')) != 2:
                print("Config Key {} is invalid".format(cfg_key))
                sys.exit(-1)
    else:
        oride_etl_cfg = None

    # Invoke the etl_engine
    run_etl(loadercode, etl_var_dict=etl_var_dict, oride_etl_cfg=oride_etl_cfg)
