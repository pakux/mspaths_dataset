#! /usr/bin/env python

#
#  Site, Age (Birthyear) : Social History
#  sex: MSPT Sociodemographic

# mpi, sex, site, birhtyear, group


import json
import pandas as pd
from glob import glob
from os.path import join, abspath
from argparse import ArgumentParser
from rich.progress import track
from msp_tables import prepare_tables, get_ids
from rich.logging import RichHandler
import logging

FORMAT = "%(message)s"
logging.basicConfig(
    level="DEBUG", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)
log = logging.getLogger("rich")


def main(bidspath:str, mspaths_dir:str):
    """
    Main function. 

    Parameters
    ----------
    bidspath : str
        Path to a specific file or directory to process.
    mspaths_dir : str
        Path to a specific file or directory to process.
    """

    # First get a list of all MPIS in the Dataset
    mpis = get_ids(bidspath)

    with open("column_names.json", "r") as file:
        table_data = json.load(file)
    
    prepared_tables = prepare_tables(mspaths_dir, bidspath, table_data)
    
    
    df = prepared_tables["MSPT Sociodemographics"]
    df["mpi"] = df["mpi"].astype(str)
    df = pd.DataFrame(df.query('mpi in @mpis and study_id==5')[['mpi', 'site', 'sex']])

    import ipdb; ipdb.set_trace()

    df["group"] = "controls" 
    
    mpis = df.mpi.to_list()
    log.debug(mpis)
    df_age = prepared_tables["Social History"].query('mpi in @mpis')[['mpi','nm_strt', 'age']]
    df_age['date'] = pd.to_datetime(df_age["nm_strt"], unit='s')
    df_age['birthyear'] = df_age.date.dt.year - df_age.age
    df_age = df_age.groupby('mpi').agg({'birthyear': lambda x: round(x.median())})
    df = pd.merge(df, df_age, on='mpi', how='left')

    df.to_csv(join(bidspath, 'participants_hc.tsv'), sep='\t', index=False)
    







if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('bidspath', type=str, help='Path to a specific file or directory to process.')
    parser.add_argument('mspathspath', type=str, help='Path to a specific file or directory to process.')
    args = parser.parse_args()

    main(args.bidspath, args.mspathspath)