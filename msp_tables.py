import json
import logging
import pandas as pd
from glob import glob
from rich import print
from rich.progress import track
from rich.logging import RichHandler
from os.path import join, abspath,basename, exists

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
    log.debug(f"query {bids_dir} => found mpis: {patlist}")
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
    log.debug(f"Reading tables from {basedir}")
    
    # if corrected is in filelist -> remove non-corrected
    remove_files = [f.replace('_CORRECTED', '') for f in filelist if 'CORRECTED' in f.upper()]
    log.debug(f"ignoring files: {remove_files}")
    filelist = [f for f in filelist if f not in remove_files]

    log.debug(f"Reding follwoing tables: {filelist}")

    df = pd.DataFrame({"mpi":[]})
    if len(filelist) > 0:
        for f in filelist:
            log.debug(f"reading {f} ")
            f_df = pd.read_csv(f, encoding="cp1252") # Some tables contain Chars that are not readable with default utf-8 codepage
            f_df['file'] = basename(f)
            df = pd.concat((df, f_df), ignore_index = True)
            
        # now change Filetype of MPI to str
        df.mpi = df.mpi.astype(str)

        if subjects is None:
            log.info("No subject selected - use all subjects")
        else:
            log.debug("filtering mpis")
            df = df.query("mpi in @subjects")
    else: 
        log.debug(f"Empty filelist - skipping")


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
            try: 
                df[pair[0]] = df[pair[0]].fillna(df[col])
                df.drop(columns=col, inplace=True)
            except KeyError:
                continue

    return df


def prepare_tables(mspaths_dir, bidsdir, tables, all_mpis:bool=False):
    """
    Collect all tables: put datacuts together into single tables, remove double entries

    table: dictionary
        name: str (basedir of the table)
        pairs: list of set

    Typically table is saved as json

    Parematers:
    ------------

    mspaths_dir: str
        Path to MSPATHS Data Tables
    bidsdir: str
        root_dir of BIDS-Dataset
    tables: dictionary
        key: tablename, value: list of set of colnames

    all_mpis: bool
        Set to true if you don't want to filter out MPIS from bids-dataset
    """

    mpis = get_ids(bidsdir) if not all_mpis else None

    prepared = {}

    for tablename, pairs in tables.items():
        df = read_mspaths_csvs(mspaths_dir, tablename, mpis)
        df = column_pairs(df, pairs)

        
        outfile = join(bidsdir, f'{tablename}.csv')
        log.debug(f"writing {tablename} to {outfile}")
        df.to_csv(outfile)
        prepared[tablename] = df

    return prepared




def create_participants_tsv(mspaths_dir, bidsdir, group:str|None = None):
    """
        - age(at baseline),
        - sex 
        - handedness
        - siteID
        - group 
        - ... 


    Parameters
    ------------
    
    mspaths_dir: str
        Path to Data Tables directory
    bidsdir: str
        Path to output-bids dir (root)
    group: str
        should be "patients" or "controls", if None it will be detetected by StudyID (001 => patients, 005 => controls)
    """
    
    with open("column_names.json", "r") as file:
        table_data = json.load(file)
    
    mpis = get_ids(bidsdir) # get all MSP-IDs from the BIDS-Dataset
    prepared_tables = prepare_tables(mspaths_dir, bidsdir, table_data)

    # Sex can be fetched from EMR and MSPT Sociodemoigraphics => both are incomplete so we combine them to get the maximum amount
    try:
        df = pd.merge(prepared_tables["EMR Sociodemographics"], prepared_tables["MSPT Sociodemographics"], on=['mpi', 'site', 'sex'], how='outer')
    except KeyError:
        df = prepared_tables["MSPT Sociodemographics"]


    all_mpis = df.drop_duplicates(subset=["mpi"]).mpi.to_list()

    medhist_df = prepared_tables["Social History"]
    if len(medhist_df) > 0:
        df = pd.merge(df, medhist_df, on=['mpi', 'site'], how='left')



    df.sex = df.sex.str.lower() # the tables are inconsistent in the use of caps
    df.sex.replace('undifferentiated', pd.NaT) #  some people have different entries in sex => remove the undifferentiated to get entries for those patients who
                                                   # have only                                                
    grouped_df = df.dropna(subset='sex').groupby('mpi').agg({'sex': 'unique'})
    indifferent_sex_mpis = grouped_df[pd.DataFrame(grouped_df).apply(lambda x: len(x.sex), axis=1) > 1].index.to_list()
    mpis_with_clear_sex = pd.DataFrame(grouped_df.query('mpi not in @indifferent_sex_mpis').sex.map(lambda x: x[0]))
    sex_df = mpis_with_clear_sex.query('mpi in @mpis')

    grouped_df = df.dropna(subset='site').groupby('mpi').agg({'site': 'unique'}, skipna=True)
    indifferent_site_mpis = grouped_df[pd.DataFrame(grouped_df).apply(lambda x: len(x.site), axis=1) > 1].index.to_list()
    mpis_with_clear_site = pd.DataFrame(grouped_df.query('mpi not in @indifferent_site_mpis').site.map(lambda x: x[0]))
    site_df = mpis_with_clear_site.query('mpi in @mpis')
    results_df = pd.merge(sex_df, site_df, on='mpi', how='outer')
    results_df = results_df.merge(pd.DataFrame({'mpi': mpis}), on='mpi', how='right')


    


    # Read Birthyear and assume
    # df = prepared_tables["EMR Sociodemographics"]
    # df["date"] = pd.to_datetime(df.effective_date, unit='s')

    if "effective_date" in df.columns:
        df["date"] = pd.to_datetime(df.effective_date, unit='s')
    elif "nm_strt" in df.columns:
        df["date"] = pd.to_datetime(df.nm_strt, unit='s')
    else:
        from ipdb import set_trace
        set_trace()    
    df["birthyear"] = df.date.dt.year - df.age
    df["birthyear"] = df.birthyear.round(0)

    results_df = results_df.merge(pd.DataFrame(df.groupby('mpi').agg({'birthyear': lambda x: round(x.median())})), on='mpi', how='left')
    # Narrow down the data set to known MPIs

    results_df = pd.DataFrame(results_df.query("mpi in @all_mpis"))
    # Now add the Group - patients/controls => based on automatic guess or given as parameter
    if group is None:
        if "888MS001" in mspaths_dir:
            group = "patients"
        elif "888MS005" in mspaths_dir:
            group = "controls"

    # results_df.query("mpi in @all_mpis")["group"] = group
    results_df["group"] = group

    return results_df




def main(mspaths_dir, bidsdir, overwrite_participants_tsv:bool=False):

    with open("column_names.json", "r") as file:

        table_data = json.load(file)

    
    prepare_tables(mspaths_dir, bidsdir, table_data)
    participants_df = create_participants_tsv(mspaths_dir, bidsdir)

    file_path = join(bidsdir, 'participants.tsv')
    if overwrite_participants_tsv:
        participants_df.to_csv(file_path, header=True, sep='\t', index=False)
    else:
        if exists(file_path):
            # Append the DataFrame to the existing file
            participants_df.to_csv(file_path, mode='a', header=False, sep='\t', index=False)
        else:
            # Save the DataFrame as a new TSV file
            participants_df.to_csv(file_path, mode='w', header=True, sep='\t', index=False)


if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('mspaths_dir', type=str, help='path of mspaths-datatables')
    parser.add_argument('bidsdir', type=str, help='resulting bidsdir')
    parser.add_argument('--group', '-g', type=str, default=None, help='Set the group of the Patient')
    args = parser.parse_args()

    main(args.mspaths_dir, args.bidsdir)

    