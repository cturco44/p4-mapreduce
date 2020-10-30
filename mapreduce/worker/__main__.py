import os
import logging
import json
import time
import click
import mapreduce.utils
import threading
import socket
from mapreduce.utils import listen_setup, tcp_socket


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
        self.sock = tcp_socket(self.worker_port)

        thread = threading.Thread(target=self.listen, args=(signals, self.sock,))
        thread.start()

        # send the register message to Master
        self.register()

        # TODO: upon receiving the register_ack message, create a new thread which will be
        # responsible for sending heartbeat messages to the Master

        if signals["shutdown"]:
            thread.join()
        
        # NOTE: the Master should ignore heartbeat messages from a worker
        # before that worker has successfully registered


    def listen(signals, sock, worker_dict):
        """Wait on a message from a socket or a shutdown signal."""
        sock.settimeout(1)
        while not signals["shutdown"]:
            message_str = listen_setup(sock)

            try:
                message_dict = json.loads(message_str)
                message_type = message_dict["message_type"]

                if message_type == "shutdown":
                    signals["shutdown"] = True

                #elif message_type == "register_ack":
                    # TODO: start sending heartbeats
                    #self.send_heartbeats()
            except json.JSONDecodeError:
                continue


    def send_tcp_message(self, message_json): 
        """Send a TCP message from the Worker to the Master."""
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


    #def send_heartbeats(self):
        # TODO: send heartbeats


    def register(self):
        """Send 'register' message from Worker to the Master."""
        register_dict = {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": self.worker_port,
            "worker_pid": self.worker_pid
        }

        message_json = json.dumps(register_dict)
        self.send_tcp_message(message_json)

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
