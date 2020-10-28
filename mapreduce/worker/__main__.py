import os
import logging
import json
import time
import click
import mapreduce.utils
import threading
import socket
from mapreduce.utils import listen, create_socket


# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Worker:
    def __init__(self, master_port, worker_port):
        logging.info("Starting worker:%s", worker_port)
        logging.info("Worker:%s PWD %s", worker_port, os.getcwd())
        
        # Class variables
        self.worker_pid = os.getpid()
        self.worker_port = worker_port
        self.master_port = master_port

        # Create new tcp socket on the worker_port and call listen(). only one listen().
        # ignore invalid messages including those that fail at json decoding
        signals = {"shutdown": False}
        self.sock = create_socket(self.worker_port)

        thread = threading.Thread(target=listen, args=(signals, self.sock,))
        thread.start()

        # send the register message to Master
        self.register()

        # TODO: upon receiving the register_ack message, create a new thread which will be
        # responsible for sending heartbeat messages to the Master

        # TODO: shutdown
        
        # NOTE: the Master should ignore heartbeat messages from a worker
        # before that worker has successfully registered



    def send_message(message_json): 
        """Send a message from the Worker to the Master."""
        # TODO: supposed to open an additional socket for sending messages to master socket?
        try:
            # create an INET, STREAMing socket, this is TCP
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            # connect to the server
            sock.connect(("localhost", self.master_port))

            sock.sendall(str.encode(message_json))
            sock.close()
        except socket.error as err:
            print("Failed to send message to Master.")
            print(err)


    def register():
        """Send 'register' message from Worker to the Master."""
        register_dict = {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": self.worker_port,
            "worker_pid": self.worker_pid
        }

        message_json = json.dumps(register_dict)
        self.send_message(message_json)

        logging.debug(
            "Worker:%s received\n%s",
            self.worker_port,
            json.dumps(register_dict, indent=2),
        )



@click.command()
@click.argument("master_port", nargs=1, type=int)
@click.argument("worker_port", nargs=1, type=int)
def main(master_port, worker_port):
    Worker(master_port, worker_port)


if __name__ == '__main__':
    main()
