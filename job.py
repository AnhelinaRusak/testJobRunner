import json
import os
import subprocess

from logger import log
from database import JobTableTools, JobTable
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
    def run_cmd_from_repository(cmd_command):
        log.info(f'Running cmd {cmd_command}')
        result = subprocess.run(cmd_command, shell=True, capture_output=True, text=True, cwd=REPOSITORY_PATH)
        if result.returncode != 0:
            log.info(result.stdout)
            log.error(result.stderr)
            raise subprocess.CalledProcessError(returncode=result.returncode, cmd=cmd_command, output=result.stdout,
                                                stderr=result.stderr)
        log.info(result.stdout)

    @staticmethod
    def adjust_path_to_os(dictionary: dict) -> dict:
        for key, value in dictionary.items():
            if isinstance(value, dict):
                Job.adjust_path_to_os(value)
            elif isinstance(value, str):
                if value.startswith("N:"):
                    dictionary[key] = value.replace("N:", "/mnt/n").replace("\\", "/")
        return dictionary

    def push_to_db(self) -> None:
        job = JobTable(uuid=self.uuid, type=self.type, status='Pending')
        self.tools.create_record(job)

    def checkout_branch(self):
        self.run_cmd_from_repository('git fetch')
        self.run_cmd_from_repository(f'git checkout {self.branch}')
        self.run_cmd_from_repository('git pull')

    def build_docker_image(self):
        self.run_cmd_from_repository('sudo docker build . -t computer_vision')

    def run_docker_container(self):
        if self.params:
            arguments = [f"--{key}='{value}'" if isinstance(value, str) else f"--{key}={value}" for key, value in self.params.items()]
        else:
            arguments = []
        command = f'bash -c "python3.10 {self.path_to_entry_point} {" ".join(arguments)}"'
        volumes = "/mnt/n:/mnt/n"
        labels = f'--label logging=promtail --label logging_jobname="{CONTAINER}"'
        cmd_command = f'sudo docker run -it --volume {volumes} {labels} --gpus={GPU_ID} computer_vision {command}'
        self.run_cmd_from_repository(cmd_command)

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
