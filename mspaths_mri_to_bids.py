#! /usr/bin/env python3

import zipfile
import logging
import shutil
import subprocess
from glob import glob
from rich import print
from os import makedirs, rename
from rich.logging import RichHandler
from os.path import join, abspath, exists, basename
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn, TimeElapsedColumn



__tmpdir="/tmp/mspaths_to_bids/"

### Setup Logging
FORMAT = "%(message)s"
logging.basicConfig(
    level="INFO", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)
log = logging.getLogger("rich")


def extract_bundle(path, bidsroot, tmpdir=__tmpdir, skip_processed=True, progress=None, task=None):
    """
    Extract a single bundle-file

    Parameters
    ----------
    path: str
        full filename of zipfile
    skip_processed: bool
        Skip this file if it was done before (see 'files_processed.csv')

    Returns
    -------
        list of extracted dicoms
    """

    if skip_processed and exists('processed_zipfiles.csv'):
        with open('processed_zipfiles.csv') as file:
            lines = [line.rstrip() for line in file]
            
            # Skip this file if already processed
            if path in lines:
                log.info(f'Already processed {path}. Skipping')
                return []

    if exists(tmpdir):
        # Clear the directory
        shutil.rmtree(tmpdir)
        makedirs(tmpdir)  # Recreate the directory
        log.info(f"Cleared and recreated the tmpdir: {tmpdir}")
    else:
        # Create the directory
        makedirs(tmpdir)
        log.info(f"Created the directory: {tmpdir}")

    try:
        with zipfile.ZipFile(path,"r") as zip_ref:
            zip_ref.extractall(tmpdir)
    except:
        log.error(f"Could not extract {path}")
        return []

    filelist = glob(join(tmpdir, '*', '*'))
    print(f'extracted files from {path}')
    if not progress is None:
        progress.update(task, total=len(filelist), completed=0)
        progress.start_task(task)
    for f in filelist:
        try:
            subject = f.split('/')[-2].split('_')[1]
            session = f.split('/')[-1]
        except:
            log.error(f'unkown file {f} in zipfile {path}. Could not extract subject/session from filename. Skipping')
            continue
        log.info(f'converting subject {subject} , session {session}')
        result = subprocess.run(['/usr/bin/dcm2niix', '-z', 'y', '-b', 'y', '-o',f ,f], capture_output=True)
       
        # Now copy the files to the bids-dir
        flair_file = glob(join(f,'*FLAIR*MS-P*.nii.gz')) # Identify FLAIR-File
        t1_file = glob(join(f, '*T1*MS-P*.nii.gz')) # Identify T1w File

        for nifti in flair_file + t1_file:
            sidecar = nifti.replace('.nii.gz', '.json')
            modality = "FLAIR" if "FLAIR" in nifti else "T1w"
            target_file = f'sub-{subject}_ses-{session}_{modality}'
            target_dir = join(bidsroot, f'sub-{subject}', f'ses-{session}', 'anat' )
            if not exists(target_dir):
                makedirs(target_dir)

            shutil.copy(nifti, join(target_dir, f'{target_file}.nii.gz'))
            if exists(sidecar):
                shutil.copy(sidecar, join(target_dir, f'{target_file}.json'))
            else:
                log.error(f'Sidecar {sidecar} does not exist. Extracted from {path}')

        if not progress is None:
            progress.update(task, advance=1)
        
    # Write Name of finally processed file to list
    with open('processed_zipfiles.csv', 'a') as f:
        f.write(f'{path}\n')

    return filelist


def extract_mri_files(source, bidsroot):

    dicom_zips = glob(join(source, 'MRI*', 'MSPATHS*_DICOM_*.zip'))
    log.debug(f'Found {len(dicom_zips)} ZIPs with DICOM-Files to extract')
    if len(dicom_zips) == 0:
        print(f"[yellow]Warning: [/yellow] could not find any Dicom-ZIP in {source}")
        return

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    ) as progress:
        zipfile_task = progress.add_task("[red]Extracting Zipfiles...", total=len(dicom_zips))
        dicom_task = progress.add_task("[dodger_blue1]Processing DICOMS", total=None)
        for zipfile in dicom_zips:
            log.debug(f'Starting to extract files from {zipfile}')
            
            progress.update(zipfile_task, advance=1, description=f"[red]Extracting Zipfile [yellow]{basename(zipfile)}")
            progress.reset(dicom_task)
            extract_bundle(zipfile, abspath(bidsroot), progress=progress, task=dicom_task)

def extract_single_zipbundle(source, bidsroot):
     with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    ) as progress:
        dicom_task = progress.add_task("[dodger_blue1]Processing DICOMS", total=None)
        extract_bundle(source, abspath(bidsroot), progress=progress, task=dicom_task) 



def cleanup_sessions(bidsroot):

    log.info(f'Finding all subjects in bidsdir {bidsroot}')
    subjects = glob(join(abspath(bidsroot), 'sub-*'))

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn()) as progress:
        renaming_task = progress.add_task("[yellow]Renaming sessions ", total=len(subjects))

        for subject in subjects:
            sessions = [basename(s).replace('ses-','') for s in glob(join(subject, 'ses-*'))]
            sessions.sort(reverse=False)

            
            sessions_tsv = join(subject, f'sub-{subject_id}_sessions.tsv')
            sessions_df = pd.DataFrame({'session_id':[], 'acq_time': []})
            if os.path.exists(sessions_tsv):
                sessions_df = pd.read_csv(sessions_tsv, header=0, sep='\t')

            for s in range(len(sessions)):

                try:
                    ses_date = datetime.datetime.strptime (sessions[s], "%Y%m%d").strftime("%Y-%m-%d")
                except ValueError:
                    log.warning(f'session ses-{sessions[s]} is not a valid date - assuming it was already converted')
                    continue

                ses_id = f'ses-{s+1:03}'
                if ses_id in sessions_df.session_id.to_list():
                    # There's already a session with this ID:
                    if ses_date in sessions_df.query('session_id == @ses_id').acq_time.to_list():
                        # This session is already in the list => ignore
                        continue
                    else
                        # Theres already a session of the same name - but different date - find a new name
                        ses_nr = s
                        while ses_id in sessions_df.session_id.to_list():
                            ses_nr = ses_nr + 1
                            ses_id = f'ses-{ses_nr+1:03}'

                sessions_df = pd.concat((sessions_df, pd.DataFrame({'session_id': [ses_id], 'acq_time': [ses_date]})), ignore_index=True)
                log.debug(f'renaming ses-{sessions[s]} to ses-{s+1:03}')

                rename(join(subject, f'ses-{sessions[s]}'), join(subject, f'ses-{s+1:03}'))

                files = glob(join(subject, ses_id, '*', f'sub*ses-{sessions[s]}*'))
                for f in files:
                    rename(f, f.replace(f'ses-{sessions[s]}', ses_id))
            # Write the sessions_tsv
            sessions_tsv = join(subject, f'sub-{subject_id}_sessions.tsv')
            sessions_df.to_csv(sessions_tsv, sep='\t', index=False)

            progress.update(renaming_task, advance=1)

def main():
    import argparse

    parser = argparse.ArgumentParser(description='convert MSPATHS Data to BIDS')
    subparsers = parser.add_subparsers(dest = 'command')

    parser.add_argument('source')
    parser.add_argument('target', default='.', help='target BIDS-Root')
    parser.add_argument('--debug', '-d', default='ERROR', help='debug-level')
    parser.add_argument('--zipfile', '-z', action='store_true', default=False, help='source is a single bundle zipfile')

    
    # Subcommand for cleanup_sessions
    cleanup_parser = subparsers.add_parser('cleanup', help='Clean up sessions')
    cleanup_parser.add_argument('target', help='Target BIDS-Root for cleanup')


    args = parser.parse_args()

    log.setLevel(args.debug)

    if args.command == 'cleanup':
        cleanup_sessions(args.target)
    elif args.zipfile:
        extract_single_zipbundle(args.source, args.target)
    else:
        extract_mri_files(args.source, args.target)



if __name__ == "__main__":
    main()
