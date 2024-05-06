import json
import os
from logger import log
from database import JobTableTools, JobTable
from git import Repo
import docker
from arguments import args


REPOSITORY_PATH = args.repository
GPU_ID = args.gpu
MACHINE = args.machine
CONTAINER = f'DASA-{MACHINE} GPU {GPU_ID}'


class JobErrorNoRetry(Exception):
    """Exception for failed job that will be given up"""
    def __init__(self, message="An error occurred"):
        self.message = message
        super().__init__(self.message)


class JobErrorRetry(Exception):
    """Exception for failed job that will be retried"""
    def __init__(self, message="An error occurred"):
        self.message = message
        super().__init__(self.message)


class Job:
    def __init__(self, generated_uuid, branch: str,  job_type: str, path_to_entry_point: str, params: dict | None) \
            -> None:
        self.uuid = generated_uuid
        self.branch = branch
        self.type = job_type
        self.path_to_entry_point = path_to_entry_point
        self.params = params
        self.tools = JobTableTools()

    @staticmethod
    def adjust_path_to_os(dictionary: dict) -> dict:
        for key, value in dictionary.items():
            if isinstance(value, dict):
                Job.adjust_path_to_os(value)
            elif isinstance(value, str):
                if value.startswith("N:") and os.name != 'nt':
                    dictionary[key] = value.replace("N:", "/mnt/n").replace("\\", "/")
                elif value.startswith('/mnt/n') and os.name == 'nt':
                    dictionary[key] = value.replace("/mnt/n", "N:").replace("/", "\\")
        return dictionary

    def push_to_db(self) -> None:
        job = JobTable(uuid=self.uuid, type=self.type, status='Pending')
        self.tools.create_record(job)

    def checkout_branch(self):
        repo = Repo(REPOSITORY_PATH)
        origin = repo.remotes.origin
        origin.fetch()
        repo.git.pull()

        repo.git.checkout(self.branch)

    @staticmethod
    def build_docker_image():
        client = docker.from_env()
        image, build_logs = client.api.build(
            path=REPOSITORY_PATH,
            dockerfile=os.path.join(REPOSITORY_PATH, 'Dockerfile'),
            tag='ComputerVision',
        )

        for chunk in build_logs:
            if 'stream' in chunk:
                for line in chunk['stream'].splitlines():
                    log.info(line)

    def run_docker_container(self):
        client = docker.from_env()
        args = [f"--{key}={value}" for key, value in self.params.items()]
        command = ['python3.10', self.path_to_entry_point] + args
        image_name = 'ComputerVision'
        container = client.containers.run(
            image_name,
            name=CONTAINER,
            detach=True,
            volumes={'/mnt/n': {'bind': '/mnt/n', 'mode': 'rw'}},
            command=command,
            devices=f'/dev/nvidia{GPU_ID}',
            labels={
                "logging": "promtail",
                "logging_jobname": CONTAINER
            }
        )
        container.wait()
        exit_code = container.attrs['State']['ExitCode']
        log.info(f"Container exited with exit code: {exit_code}")

    def build_and_run_docker(self):
        repo = Repo(REPOSITORY_PATH)
        repo.git.fetch()
        repo.git.pull()

        repo.git.checkout(self.branch)

    def run(self) -> None:
        job = self.tools.get_record(self.uuid)
        job.status = 'In progress'
        job.container = CONTAINER
        job.machine = MACHINE
        self.tools.commit_changes()
        try:
            self.checkout_branch()
            self.build_docker_image()
            self.run_docker_container()
        except Exception as e:
            log.error(f'Job failed with error {e}')
            job.status = 'Error'
            if job.retry >= 2:
                raise JobErrorNoRetry('Reached maximum number of attempts. Giving up on a job.')
            job.retry += 1
            raise JobErrorRetry('Job failed. Attempting to retry.')
        finally:
            self.tools.commit_changes()

    @classmethod
    def get_job_from_message(cls, message: bytes):
        job_dict = cls.adjust_path_to_os(json.loads(message))
        job = cls(job_dict['uuid'], job_dict['branch'], job_dict['type'], job_dict['path_to_entry_point'],
                  job_dict['job'])
        assert job.type in ['training', 'evaluation', 'custom'], 'Job does not support this type'
        return job
