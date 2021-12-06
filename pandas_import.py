# Import rucio global accounting dump into pandas DataFrame
# Dump at http://rucio-analytix-01.cern.ch:8080/GLOBALACCOUNTING/GetFileFromHDFS?date=2020-05-17
# (only accessible inside CERN)
# Fields defined here: https://github.com/rucio/tools/blob/master/hadoop/spark/GlobalAccounting/src/main/scala/rucio/GlobalAccounting.scala#L648

import pandas as pd
import numpy as np
import dask.dataframe as dd

def run_import(filename):

    pd.options.display.max_colwidth = 500
    headers = [
            "scope",
            "name",
            "accessed_at",
            "created_at",
            "length",
            "bytes",
            "prim_t0_files",
            "prim_t0_accessed_files",
            "prim_t1_files",
            "prim_t1_accessed_files",
            "prim_t2_files",
            "prim_t2_accessed_files",
            "secs_t0_files",
            "secs_t0_accessed_files",
            "secs_t1_files",
            "secs_t1_accessed_files",
            "secs_t2_files",
            "secs_t2_accessed_files",
            "tape_t0_files",
            "tape_t0_accessed_files",
            "tape_t1_files",
            "tape_t1_accessed_files",
            "prim_t0_bytes",
            "prim_t0_accessed_bytes",
            "prim_t1_bytes",
            "prim_t1_accessed_bytes",
            "prim_t2_bytes",
            "prim_t2_accessed_bytes",
            "secs_t0_bytes",
            "secs_t0_accessed_bytes",
            "secs_t1_bytes",
            "secs_t1_accessed_bytes",
            "secs_t2_bytes",
            "secs_t2_accessed_bytes",
            "tape_t0_bytes",
            "tape_t0_accessed_bytes",
            "tape_t1_bytes",
            "tape_t1_accessed_bytes",
            "prim_repl_factor_t0",
            "prim_repl_factor_t1",
            "prim_repl_factor_t2",
            "secs_repl_factor_t0",
            "secs_repl_factor_t1",
            "secs_repl_factor_t2",
            "tape_repl_factor_t0",
            "tape_repl_factor_t1",
            "datatype",
            "prod_step",
            "run_number",
            "project",
            "stream_name",
            "version",
            "campaign",
            "events"
            ]

    dtypes = {'scope': 'str',
              'name': 'str',
              'datatype': 'str',
              'prod_step': 'str',
              'project': 'str',
              'stream_name': 'str',
              'version': 'str',
              'campaign': 'str'}

    # Lazy read to reduce memory consumption
    df = dd.read_csv(filename, sep='\t', names=headers, dtype=dtypes, assume_missing=True)
 
    # remove test and user/group datasets
    df = df[~df['scope'].str.contains('tests')]
    df = df[~df['scope'].str.contains('user.')]
    df = df[~df['scope'].str.contains('group.')]

    # remove sub datasets
    df = df[~df['name'].str.contains('_sub')]

    # convert to pandas df
    df = df.compute()

    # extract tid
    df['tid'] = pd.to_numeric(df['name'].str.split('tid', expand=True)[1].str.split('_', expand=True)[0], errors='coerce')

    # do some aggregation
    df['prim_files'] = df['prim_t0_files'] + df['prim_t1_files'] + df['prim_t2_files']
    df['sec_files'] = df['secs_t0_files'] + df['secs_t1_files'] + df['secs_t2_files']
    df['tape_files'] = df['tape_t0_files'] + df['tape_t1_files']
    df['prim_bytes'] = df['prim_t0_bytes'] + df['prim_t1_bytes'] + df['prim_t2_bytes']
    df['sec_bytes'] = df['secs_t0_bytes'] + df['secs_t1_bytes'] + df['secs_t2_bytes']
    df['tape_bytes'] = df['tape_t0_bytes'] + df['tape_t1_bytes']
    df['prim_bytes_tb'] = df['prim_bytes']/1000/1000/1000/1000
    df['sec_bytes_tb'] = df['sec_bytes']/1000/1000/1000/1000
    df['tape_bytes_tb'] = df['tape_bytes']/1000/1000/1000/1000
    df['prim_repl_factor'] = df['prim_repl_factor_t0'] + df['prim_repl_factor_t1'] + df['prim_repl_factor_t2']
    df['sec_repl_factor'] = df['secs_repl_factor_t0'] + df['secs_repl_factor_t1'] + df['secs_repl_factor_t2']
    df['tape_repl_factor'] = df['tape_repl_factor_t0'] + df['tape_repl_factor_t1']
    df['tbytes'] = df['bytes']/1000/1000/1000/1000
    df['avg_bytes'] = df['bytes']/df['length']
    df['access_time'] = pd.to_datetime(df.accessed_at, unit='ms')
    df['creation_time'] = pd.to_datetime(df.created_at, unit='ms')

    return df

