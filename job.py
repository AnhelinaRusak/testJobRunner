"""Module that handles job requests. It fetches changes from repository, checkouts branch, pulling changes,
building docker from it and then runs docker passing params and entry_point"""
import base64
import json
import os
import subprocess

from arguments import args
from database import JobTableTools, JobTable
from logger import log

REPOSITORY_PATH = args.repository
GPU_ID = args.gpu
MACHINE = f'DASA-{args.machine}'
CONTAINER = f'{MACHINE} GPU {GPU_ID}'


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
    """Class that handles job and container"""
    def __init__(self, generated_uuid, branch: str,  job_type: str, path_to_entry_point: str, params: dict | None) \
            -> None:
        self.uuid = generated_uuid
        self.branch = branch
        self.type = job_type
        self.path_to_entry_point = path_to_entry_point
        self.params = params
        self.tools = JobTableTools()

    @staticmethod
    def get_base_docker_run_command():
        """Get base docker command configuration"""
        volumes = 'drive_n' if os.name == 'nt' else "/mnt/n:/mnt/n"
        labels = f'--label logging=promtail --label logging_jobname="{CONTAINER}"'
        docker_cmd = 'docker run -it' if os.name == 'nt' else 'sudo docker run -it'
        return f'{docker_cmd} --volume {volumes} {labels} --gpus={GPU_ID} --shm-size=20gb computer_vision_{GPU_ID}'

    @staticmethod
    def run_command_from_repository(cmd_command):
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
        self.run_command_from_repository('git fetch')
        self.run_command_from_repository(f'git checkout {self.branch}')
        self.run_command_from_repository('git pull')

    def build_docker_image(self):
        self.run_command_from_repository(f'sudo docker build . -t computer_vision_{GPU_ID}')

    def run_docker_container(self):
        arguments = []
        if self.type != 'custom':
            for key, value in self.params.items():
                json_object = json.dumps(value)
                encoded_bytes = base64.b64encode(json_object.encode('utf-8'))
                encoded_string = encoded_bytes.decode('utf-8')
                arguments.append(f'--{key}={encoded_string}')
        elif self.params:
            arguments = [f"--{key}='{value}'" if isinstance(value, str) else f"--{key}={value}"
                         for key, value in self.params.items()]

        python_command = (f'bash -c "PYTHONPATH=/ComputerVisionAI/src python3.10 {self.path_to_entry_point} '
                          f'{" ".join(arguments)}"')
        cmd_command = f'{self.get_base_docker_run_command()} {python_command}'
        self.run_command_from_repository(cmd_command)

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
            job.status = 'Finished'
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
        assert job.type in ['training', 'custom'], 'Job does not support this type'
        return job
