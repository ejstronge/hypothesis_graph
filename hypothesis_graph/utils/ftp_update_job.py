# -*- coding: utf-8 -*-
"""
   ftp_update_job.py
   =================

   Script for retrieving Medline records from the NLM FTP server.  Run
   this as a cron job.

   NOTE: This script requires that a netrc file exist and contain
   only an entry for the NLM public server. See `man 5 netrc` for details
   on the netrc file format.

   (c) 2014, Edward J. Stronge
   Available under the GPLv3 - see LICENSE for details.
"""
import argparse
from collections import namedtuple
import hashlib
import ftplib
import netrc
from os import path
import re
import StringIO
import sys
import traceback
import time

from send_ses_message.send_smtp_ses_email import \
    get_smtp_parameters, get_server_reference

import ftp_update_db as ftp_db

FTPConnectionParams = namedtuple(
    'FTPConnectionParams', 'host user account password')

FTPFileParams = namedtuple(
    'FTPFileParams', 'modification_date size unique_file_id filename')


"""
Examples of an mlsd listing

modify=20131125174213;perm=adfr;size=24847843;type=file;unique=4600001UE9FE;UNIX.group=183;UNIX.mode=0644;UNIX.owner=505; medline14n0745.xml.gz
modify=20131125174556;perm=adfr;size=63;type=file;unique=4600001UEA02;UNIX.group=183;UNIX.mode=0644;UNIX.owner=505; medline14n0002.xml.gz.md5
"""


def parse_mlsd(line):
    """
    Parses lines of text from an FTP directory listing (see examples above)
    """
    metadata_string, filename = line.split()
    metadata = {}
    for param in metadata_string.split(';'):
        if not param:
            continue
        k, v = param.split('=')
        metadata[k] = v
    if metadata['type'] != 'file':
        return
    return FTPFileParams(
        metadata['modify'], metadata['size'], metadata['unique'], filename)


def get_file_listing(connection, server_dir, skip_patterns=None):
    """
    Returns tuples of file information for each file in server_dir.

    Directories are automatically removed from this listing (see
    `parse_mlsd`).

    `skip_patterns` can be a collection of regex patterns that match
    undesired files. It defaults to `(r'stats\.html$', r'\.dat$')`.
    """
    # This should definitely be done line-wise but ftplib complains when
    # the NLM server changes mode from BINARY to ASCII and back again.
    # Using StringIO is a workaround.
    #
    # Eventually, should try using ftplib sendcmd to change mode before
    # requesting a test file and reset the mode before requesting a
    # binary file.
    file_listing = StringIO.StringIO()
    connection.retrbinary('MLSD %s' % server_dir, file_listing.write)
    file_listing.seek(0)

    if skip_patterns is None:
        skip_patterns = (r'stats\.html$', r'\.dat$')
    skip_patterns = tuple(re.compile(p) for p in skip_patterns)

    for line in file_listing.readlines():
        listing = parse_mlsd(line)
        if listing is None:
            continue
        for p in skip_patterns:
            if p.match(listing.filename) is not None:
                continue
        yield listing


def retrieve_nlm_files(
        connection, server_dir, output_dir, db_con, limit=0):
    """
    Download new files from path `server_dir` to `output_dir`.
    Only retrieve `limit` files if limit is greater than 0.

    `connection` is an ftplib.FTP object.

    Returns a dict with the fields from FTPFileParams and
    the following keys:

        download_date
        observed_md5 - calculated md5 has for the referenced file
        output_path - path on local machine for the referenced file
    """
    connection.cwd(server_dir)
    # Not pretty, but I'm just getting a list of all the IDs I've
    # already downloaded. This shouldn't ever exceed a few thousand
    downloaded_files = ftp_db.get_downloaded_file_unique_ids(db_con)
    retrieved_files = []
    output_dir = path.abspath(output_dir)
    try:
        for i, file_info in enumerate(
                get_file_listing(connection, server_dir)):
            # XXX Just for debugging
            if limit > 0 and i > limit:
                break
            if file_info.unique_file_id in downloaded_files:
                continue
            output_path = path.join(output_dir, file_info.filename)
            with open(output_path, 'wb+') as new_file:
                connection.retrbinary(
                    'RETR %s' % file_info.filename, new_file.write)
                new_file.seek(0)
                observed_md5 = hashlib.md5()
                observed_md5.update(new_file.read())
            file_info_dict = file_info._asdict()
            file_info_dict.update(
                download_date=time.strftime('%Y%m%d%H%M%S'),
                observed_md5=observed_md5.digest(),
                output_path=output_path)
            retrieved_files.append(file_info_dict)
    finally:
        # Record successful downloads even after a download failure
        ftp_db.record_downloads(retrieved_files, db_con)
    return retrieved_files


def send_smtp_email(_from, to, msg, server_cfg):
    """Send `msg` to email address `to`, using the parameters
       in server_cfg.
    """
    smtp_params = get_smtp_parameters(server_cfg)
    smtp_con = get_server_reference(*smtp_params)

    smtp_con.sendmail(
        _from, to,
        """From: {from}\r\n
           To: {to}\r\n

           {msg}
        """ % {'from': _from, 'to': to, 'msg': msg})


def handle_args():
    """Parse command-line arguments to this script"""
    parser = argparse.ArgumentParser(
        description="""Script to download new files from the NLM public
                       FTP server.
                    """)
    # Server settings
    parser.add_argument(
        '-n', '--netrc', default='~/.netrc',
        help="""netrc file containing login parameters for the NLM
                server. See `man 5 netrc` for details on generating this
                file.
             """)
    parser.add_argument(
        'server_data_dir',
        help='Directory containing desired files on the NLM FTP server')
    parser.add_argument(
        '-l', '--limit', type=int, default=0,
        help='Only download LIMIT files.')

    # Download settings
    parser.add_argument(
        '-d', '--download_database', default='~/.ftp_download_db',
        help='Path to SQLite database detailing past downloads')
    parser.add_argument(
        '-o', '--output_dir', default='~/medline_data',
        help='Directory where downloads will be saved')
    parser.add_argument(
        '-x', '--export_dir', default='~/medline_data_exports',
        help="""Directory where data to be retrieved by the
                `hypothesis_graph application server are staged.
             """)
    # Sending debug emails
    parser.add_argument(
        '--email_debugging', default=False, action='store_true',
        help="Send debugging emails. Defaults to FALSE.")
    parser.add_argument(
        '--from_email', required=False, help="FROM field for debugging emails")
    parser.add_argument(
        '--to_email', required=False, help="TO field for debugging emails")

    return parser.parse_args()


def main():
    """Connect to the NLM server and download all new files"""
    args = handle_args()

    # FTP connection
    NLM_NETRC = netrc.netrc(file=path.expanduser(args.netrc))
    assert len(NLM_NETRC.hosts.keys()
               ) == 1, "The netrc file should contain only one record"
    for server, params in NLM_NETRC.hosts.items():
        FTP_PARAMS = FTPConnectionParams(*([server] + list(params)))
    ftp_connection = ftplib.FTP(
        host=FTP_PARAMS.host, user=FTP_PARAMS.user, passwd=FTP_PARAMS.password)

    def get_exception_text():
        return traceback.print_tb(sys.exc_info[2])
    try:
        retrieve_nlm_files(
            connection=ftp_connection, server_dir=args.server_data_dir,
            output_dir=args.output_dir, limit=args.limit,
            db_con=ftp_db.initialize_download_database_connection('test.db'))
    except Exception as e:
        if args.email_debugging:
            send_smtp_email(
                args.from_email, args.to_email, server_cfg=args.smtp_cfg,
                msg="""
                At {date}, attempt to download new files from
                {server_dir} failed.

                Exception text: {exception_text}
                Traceback text: {traceback_text}
                """.format(date=time.strftime('%Y%m%d%H%M%S'),
                           server_dir=args.server_data_dir,
                           exception_text=e.args,
                           traceback=get_exception_text()))
        raise
    success_email = "Downloaded all new files from %s" % args.server_data_dir

    # TODO Move the downloaded files to the export directory and send an
    # email when this is done

    # ftp_db.record_files_to_export()
    # ftp_db.check_exported_file_directory()


if __name__ == '__main__':
    main()
