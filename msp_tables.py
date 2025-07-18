import json
import logging
import pandas as pd
from glob import glob
from rich import print
from rich.progress import track
from rich.logging import RichHandler
from os.path import join, abspath,basename

__author__ = "Paul Kuntke"
__license__ = "BSD(3-Clause)"


"""
This script is intented to read 
 - age(at baseline),
 - sex 
 - handedness
 - siteID
 - ... 
from the original MSPATHS-Dataset and write them e.g. directly into the
participants.tsv for further use

"""

FORMAT = "%(message)s"
logging.basicConfig(
    level="DEBUG", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)
log = logging.getLogger("rich")


def get_ids(bids_dir):
    """
    Find all subject-ids from bids-dir
    """
    patlist = [p.replace('sub-','').split('/')[-1] for p in glob(join(bids_dir, 'sub-*'))]

    return patlist

def read_mspaths_csvs(basedir:str, table:str,  subjects:list[str]|None = None):
    """
    Read original tables from mspaths-csvs

        basedir
           - table
                 - tablename_v00x.csv:
                    columns[0..x]

    Parameters
    ----------
        basedir: str
            Path to "Data Tables" containing subfolders with tables (csvs)
        table_dir: str
            Select one table within basedir
        subjects: list or None
            Select SubjectIDs from Tables, if None all will be selected, default: None
            
    """
    filelist = glob(join(abspath(basedir), table, '*_v0*.csv'))
    
    # if corrected is in filelist -> remove non-corrected
    remove_files = [f.replace('_CORRECTED', '') for f in filelist if 'CORRECTED' in f.upper()]
    log.debug(f"ignoring files: {remove_files}")
    filelist = [f for f in filelist if f not in remove_files]


    df = pd.DataFrame()
    for f in filelist:
        log.debug(f"reading {f} ")
        f_df = pd.read_csv(f, encoding="cp1252") # Some tables contain Chars that are not readable with default utf-8 codepage
        f_df['file'] = basename(f)
        df = pd.concat((df, f_df), ignore_index = True)

    if subjects is None:
        log.info("No subject selected - use all subjects")
    else:
        df = df.query("mpi in @subjects")

    return df


def column_pairs(df:pd.DataFrame, column_pairs:list):
    """
    Some of the columns have changed their Names over the different dataset-versions (v001..v017)

    This function is meant to give the different names of columns that represent the
    same. To match them together give a list of pairs. The resulting DataFrame columns are named by the first of your pairs.

    e.g.:
        column_pairs=[("med_strt", "dmt_sdt"), ("med_end", "dmt_end")]
        will result in columns "med_strt", "med_end"

    Parameters
    ------------
    df: pandas DataFrame
        The DataFrame you want to change
    column_pairs: list of set
        The column_pairs

    """
    for pair in column_pairs:
        for col in pair[1:]:
            # Fill in empty cells with values from other cols from the pairs
            df[pair[0]] = df[pair[0]].fillna(df[col])
            df.drop(columns=col, inplace=True)

    return df


def prepare_tables(mspaths_dir, bidsdir, tables):
    """
    Collect all tables: put datacuts together into single tables, remove double entries

    table: dictionary
        name: str (basedir of the table)
        pairs: list of set
            
    """
    mpis = get_ids(mspaths_dir)

    # MSPT-Sociodemograph
    
    for tablename, pairs in tables.items():
        df = read_mspaths_csvs(mspaths_dir, tablename, mpis)
        df = column_pairs(df, pairs)

        df.to_csv(join(bidsdir, f'{tablename}.csv'))


def main(mspaths_dir, bidsdir):

    with open("column_names.json", "r") as file:
        table_data = json.load(file)

    prepare_tables(mspaths_dir, bidsdir, table_data)


def create_participants_tsv(mspaths_dir, bidsdir):
    """
        - age(at baseline),
        - sex 
        - handedness
        - siteID
        - ... 
    """

    mpis = get_ids(mspaths_dir) # get all MSP-IDs from the BIDS-Dataset
    
    
    
    


if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parse.add_argument('mspaths_dir', type=str, help='path of mspaths-datatables')
    parse.add_argument('bidsdir', type=str, help='resulting bidsdir')
