#! {/}
import json
from glob import glob
from msp_tables import prepare_tables, read_mspaths_csvs, column_pairs


def main():
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument(
        "--mspaths_dir", default=".", type=str, help="root dir of mspaths tables"
    )
    parser.add_argument("--table", "-t", type=str, help="Table Name to extract")
    parser.add_argument(
        "--mpis",
        "-i",
        type=str,
        default=None,
        help="List of MPIS that will be in resulting table",
    )

    args = parser.parse_args()

    with open("column_names.json", "r") as file:
        table_data = json.load(file)

    for tablename, pairs in table_data.items:
        df = read_mspaths_csvs(args.mspaths_dir, tablename, mpis)
        df = column_pairs(df, pairs)


if __name__ == "__main__":
    main()
