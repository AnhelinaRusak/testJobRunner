from typing import Any

import pika

from arguments import args
from credentials import get_credentials
from job import Job, JobErrorRetry, JobErrorNoRetry
from logger import log


class RabbitMQListener:
    """Class for connection to RabbitMQ"""

    def __init__(self) -> None:
        """Create connection"""
        credentials_mq = get_credentials('RabbitMQ')
        credentials = pika.PlainCredentials(credentials_mq['user'], credentials_mq['password'])
        parameters = pika.ConnectionParameters(credentials_mq['host'], int(credentials_mq['port']),
                                               credentials_mq['v_host'], credentials, heartbeat=0)

        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=1)

    @staticmethod
    def job_request_handler(channel: pika.adapters.blocking_connection.BlockingChannel,
                            method: pika.spec.Basic.Deliver, ___: Any, body: bytes) -> None:
        """Handle callback from queue and starts training"""
        log.info(f"Received: {body!r}")
        try:
            job = Job.get_job_from_message(body)
            log.info(f'Received job with uuid={job.uuid}')
            job.run()
            log.info("Job finished.")
            channel.basic_ack(method.delivery_tag)
        except JobErrorNoRetry as e:
            log.error(e)
            channel.basic_ack(method.delivery_tag)
        except JobErrorRetry as e:
            log.error(e)
            channel.basic_nack(method.delivery_tag)
        except Exception as e:
            log.error(f'Unhandled error outside running job {e}')
        log.info("Cleaning finished. Start listening.")

    def listen_to_queue(self, queue):
        self.channel.basic_consume(queue=queue, on_message_callback=self.job_request_handler, auto_ack=False)
        log.info('Start listening')
        self.channel.start_consuming()


if __name__ == '__main__':
    listener = RabbitMQListener()
    listener.listen_to_queue(args.queue)
