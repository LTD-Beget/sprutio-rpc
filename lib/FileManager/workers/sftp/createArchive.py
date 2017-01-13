from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
import traceback


class CreateArchive(BaseWorkerCustomer):
    def __init__(self, params, session, *args, **kwargs):
        super(CreateArchive, self).__init__(*args, **kwargs)

        self.path = params.get('path')
        self.session = session
        self.type = params.get('type', 'zip')
        self.file_items = params.get('files', [])

        self.params = params

    def run(self):
        try:
            self.preload()
            sftp = self.get_sftp_connection(self.session)

            archive_type = self.get_archive_type(self.type)
            if not archive_type:
                raise Exception("Unknown archive type")

            archive_name = "\"" + self.path + "." + archive_type + "\""

            files_string = "\"" + "\" \"".join([d['path'][2:] for d in self.file_items]) + "\""
            full_command = " ".join([self.get_command(self.type), archive_name, files_string])

            status = sftp.run(full_command)

            if not status.succeeded:
                raise Exception("Error on server. Error code: %s. Full command: %s. Stdout: %s. Stderr: %s" %
                                (status.returncode, full_command, status.stdout, status.stderr))

            progress = {
                'percent': 100,
                'text': '100%'
            }
            result = {
                "archive": archive_name
            }

            self.on_success(self.status_id, data=result, progress=progress, pid=self.pid, pname=self.name)

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error("SFTP createArchive error = {}".format(result))

            self.on_error(self.status_id, result, pid=self.pid, pname=self.name)

    @staticmethod
    def get_archive_type(extension):
        archive_type = False
        if extension == 'zip':
            archive_type = 'zip'
        elif extension == 'gzip':
            archive_type = 'tar.gz'
        elif extension == 'bz2':
            archive_type = 'tar.bz2'
        elif extension == 'tar':
            archive_type = 'tar'
        return archive_type

    @staticmethod
    def get_command(archive_type):
        command = ""
        if archive_type == "zip":
            command = "zip -r"
        elif archive_type == "gzip":
            command = "tar -zcvf"
        elif archive_type == "bz2":
            command = "tar -jcvf"
        elif archive_type == "tar":
            command = "tar -cvf"

        return command
