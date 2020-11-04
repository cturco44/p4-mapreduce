import os
import logging
import json
import time
import click
import mapreduce.utils
import threading
import socket
import pathlib
import subprocess
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
        self.state = "not_ready"
        self.job_counter = 0 # TODO: increment when done with current job

        # Create new tcp socket on the worker_port and call listen(). only one listen().
        # ignore invalid messages including those that fail at json decoding
        signals = {"shutdown": False}
        self.sock = tcp_socket(self.worker_port)
        thread = threading.Thread(target=self.listen, args=(signals,))
        thread.start()

        # send the register message to Master
        self.register()

        # TODO: upon receiving the register_ack message, create a new thread which will be
        # responsible for sending heartbeat messages to the Master

        # FOR TESTING
        count = 0
        while not signals["shutdown"]:
            time.sleep(1)
            count += 1
            if count > 15:
                break

        #if signals["shutdown"]:
        thread.join()
        self.sock.close()
        print(threading.enumerate())

        # NOTE: the Master should ignore heartbeat messages from a worker
        # before that worker has successfully registered


    def listen(self, signals):
        """Wait on a message from a socket or a shutdown signal."""
        self.sock.settimeout(1)
        while not signals["shutdown"]:
            message_str = listen_setup(self.sock)

            if message_str == "":
                continue

            try:
                message_dict = json.loads(message_str)
                message_type = message_dict["message_type"]
                # TODO: for testing
                print(message_dict)

                if message_type == "shutdown":
                    signals["shutdown"] = True
                    break
                    #thread.join()
                    #self.sock.close()

                elif message_type == "register_ack":
                    # TODO: start sending heartbeats
                    self.state = "ready"
                    #self.send_heartbeats()

                elif message_type == "new_worker_job":
                    self.state = "busy"
                    self.new_worker_job(message_dict)
                    self.state = "ready"


            except json.JSONDecodeError:
                continue


    def new_worker_job(self, message_dict):
        """Handles mapping stage."""
        executable = message_dict["executable"]
        mapper_output_dir = pathlib.Path("tmp/job-" + str(self.job_counter) + "/mapper-output")
        output_files = []

        for file in message_dict["input_files"]:
            input_file = open(file)
            output_dir = str(mapper_output_dir/self.input_file_name(file))

            output_files.append(output_dir)
            output_file = open(output_dir, "w")

            subprocess.run(args=[executable], stdin=input_file, stdout=output_file) # shell?

        job_dict = {
            "message_type": "status",
            "output_files": output_files,
            "status": "finished",
            "worker_pid": self.worker_pid
        }

        job_json = json.dumps(job_dict)

        self.send_tcp_message(job_json)


    def input_file_name(self, file_path):
        """Return only name of input file, given entire input path."""
        dirs = file_path.split("/")
        return dirs[-1]


    def send_tcp_message(self, message_json): 
        """Send a TCP message from the Worker to the Master."""
        try:
            # create an INET, STREAMing socket, this is TCP
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            # connect to the server
            sock.connect(("localhost", self.master_port))

            sock.sendall(message_json.encode('utf-8'))
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
