"""
This script recursively copies files from a local directory to the SD2e Agave
server replicating the directory structure.

Requires agavepy.
"""
import os
import argparse
import getpass
from agavepy.agave import Agave

class AgaveWrapper:
    """
    An adapter for writing files to TA3 file space using SD2e Agave.

    Methods shadow the S3 interface in boto3, if you swap "directory" for "bucket"
    """

    def __init__(self, **params):
        """
        Initialize a DataManager object.

        Arguments:
          params (string) rootpath  the root path on the Agave server
          params (string) system_id  the system id for the Agave client
          params (string) username  the username for connecting to the server
          params (string) password  the password for connecting to the server
        """
        self._rootpath = params['rootpath']
        self._system_id = params['system_id']

        self._agave = Agave(
            api_server='https://' + os.environ['AGAVE_SERVER'],
            username=params['username'],
            password=params['password'],
            client_name=os.environ['AGAVE_CLIENT'],
            api_key=os.environ['AGAVE_API_KEY'],
            api_secret=os.environ['AGAVE_API_SECRET']
        )

    def make_directory(self, path):
        """
        Creates a subdirectory from the root path.
        (not necessary to include the root path in the argument)

        Argument:
          path (string) the path from the root path to be created
        """
        self._agave.files.manage(
            body={'action': 'mkdir', 'path': path},
            systemId=self._system_id,
            filePath=self._rootpath
        )

    def upload_file(self, file_path, dest_path, filename):
        """
        Uploads the file to the given destination.

        Arguments:
          file_path (string)  the path to the file to upload
          dest_path (string)  the destination path on the server
          filename (string)  the name of the file to upload
        """
        with open(file_path, 'rb') as file:
            self.upload_fileobj(file, dest_path, filename)

    def upload_fileobj(self, file_obj, dest_path, filename):
        """
        Uploads the file object to the given destination.

        Arguments:
          file_object (file object)  the object for the file to upload
          dest_path (string)  the destination path on the server
          filename (string)  the name of the file to upload
        """
        qualified_path = os.path.join(self._rootpath, dest_path)
        self._agave.files.importData(
            systemId=self._system_id,
            filePath=qualified_path,
            fileToUpload=file_obj,
            fileName=filename
        )


def copy_from(local_path, agave, remote_path):
    """
    Copies a local directory structure and contents to the rootpath on the Agave 
    server.
    """
    agave.make_directory(remote_path)
    for file in os.listdir(local_path):
        local_file_path = os.path.join(local_path, file)
        remote_file_path = os.path.join(remote_path, file)
        if (os.path.isdir(local_file_path)):
            copy_from(local_file_path, agave, remote_file_path)
        elif os.path.isfile(local_file_path):
            agave.upload_file(local_file_path, remote_path, file)

def main():
    """
    Parses the command-line arguments for the directory to be copied, creates
    the AgaveWrapper object to point to data-sd2e-community/sample/biofab, and
    then copies the directory and its contents to that location.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", help="the directory to be copied to the server")
    args = parser.parse_args()

    local_path = os.path.normpath(args.directory)
    remote_path = os.path.basename(local_path) 

    agave = AgaveWrapper(
            rootpath='/sample/biofab',
            system_id='data-sd2e-community',
            username=getpass.getuser(),  # use login username
            password=getpass.getpass()  # will prompt for password
        )

    copy_from(local_path, agave, remote_path)


if __name__ == "__main__":
    main()