"""Program."""
import json
import socket
import logging
import threading
import time
import os
import pathlib
import subprocess
import click
from mapreduce.utils import listen_setup, tcp_socket


# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Worker:
    """Worker Class."""

    def __init__(self, master_port, worker_port):
        """Docstring."""
        logging.info("Starting worker:%s", worker_port)
        logging.info("Worker:%s PWD %s", worker_port, os.getcwd())

        # Class variables
        # self.worker_pid = os.getpid()
        worker_pid = os.getpid()
        # self.worker_port = worker_port
        self.master_port = master_port
        self.state = "not_ready"
        self.job_type = "idle"
        self.job_json = None
        self.job_counter = 0  # increment when done with current job
        self.heartbeat_thread = None

        # Create new tcp socket on the worker_port and call listen().
        # only one listen().
        # ignore invalid messages including those that fail at json decoding
        self.shutdown = False
        sock = tcp_socket(worker_port)
        worker_thread = threading.Thread(target=self.listen,
                                         args=(sock, worker_port, worker_pid))
        worker_thread.start()

        check_job_thread = threading.Thread(target=self.check_if_job,
                                            args=(worker_pid,))
        check_job_thread.start()

        # if signals["shutdown"]:
        check_job_thread.join()
        print("f")
        worker_thread.join()
        print("g")
        sock.close()
        print("h")
        # for testing
        print(worker_pid, " alive? ", worker_thread.is_alive())
        # print(len(threading.enumerate()))
        # print(threading.enumerate())

        # NOTE: the Master should ignore heartbeat messages from a worker
        # before that worker has successfully registered

    def listen(self, sock, worker_port, worker_pid):
        """Wait on a message from a socket or a shutdown signal."""
        # send the register message to Master
        self.register(worker_port, worker_pid)
        sock.settimeout(1)
        while not self.shutdown:
            message_str = listen_setup(sock)
            if message_str == "":
                continue
            try:
                message_dict = json.loads(message_str)
                message_type = message_dict["message_type"]
                # for testing
                print("Worker {} recv msg: ".format(
                    worker_pid), message_dict)

                if message_type == "shutdown":
                    self.shutdown = True
                    if self.heartbeat_thread is not None:
                        self.heartbeat_thread.join()
                        print("e")

                elif message_type == "register_ack":
                    self.heartbeat_thread = threading.Thread(
                        target=self.send_heartbeats, args=(worker_pid,))
                    self.state = "ready"
                    self.heartbeat_thread.start()

                elif message_type == "new_worker_job":
                    self.job_type = "mapreduce"
                    self.job_json = message_dict

                elif message_type == "new_sort_job":
                    self.job_type = "group"
                    self.job_json = message_dict

            except json.JSONDecodeError:
                continue

    def check_if_job(self, worker_pid):
        """Check if job."""
        while not self.shutdown:
            if self.state == "ready" and self.job_type != "idle":
                self.state = "busy"
                msg_dict = self.job_json
                work = (self.new_worker_job
                        if self.job_type == "mapreduce" else self.new_sort_job)
                work(msg_dict, worker_pid)
            time.sleep(0.1)

    def new_worker_job(self, message_dict, worker_pid):
        """Handle mapping and reducing stage."""
        executable = message_dict["executable"]
        print(executable)
        mapper_output_dir = pathlib.Path(message_dict['output_directory'])
        mapper_output_dir.mkdir(parents=True, exist_ok=True)
        output_files = []

        for file in message_dict["input_files"]:
            file = pathlib.Path(file)
            output_dir = mapper_output_dir / file.stem
            output_files.append(str(output_dir))
            output_file = open(output_dir, "w")
            with open(file, 'r') as input_file, \
                    open(output_dir, "w") as output_file:
                # not sure if check = true or false
                subprocess.run(args=["chmod", "+x", executable], check=True)
                # shell? TODO
                subprocess.run(args=[executable],
                               stdin=input_file,
                               stdout=output_file, check=True)

        job_dict = {
            "message_type": "status",
            "output_files": output_files,
            "status": "finished",
            "worker_pid": worker_pid
        }

        job_json = json.dumps(job_dict)
        print(job_json)

        self.send_tcp_message(job_json)
        self.state = "ready"
        self.job_type = "idle"

    def new_sort_job(self, message_dict, worker_pid):
        """Handle grouping stage."""
        output_file = pathlib.Path(message_dict["output_file"])
        output_file.touch(exist_ok=True)
        lines = []
        for file in message_dict["input_files"]:
            open_file = open(file, 'r')
            lines.extend(open_file.readlines())
            open_file.close()
        lines.sort()
        write_file = open(message_dict["output_file"], 'w')
        for line in lines:
            write_file.write(line)
        write_file.close()

        job_dict = {
            "message_type": "status",
            "output_file": str(message_dict["output_file"]),
            "status": "finished",
            "worker_pid": worker_pid
        }
        job_json = json.dumps(job_dict, indent=2)
        print(job_json)

        self.send_tcp_message(job_json)
        self.state = "ready"
        self.job_type = "idle"

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

    def send_heartbeats(self, worker_pid):
        """Send heartbeats."""
        msg = {
            "message_type": "heartbeat",
            "worker_pid": worker_pid
        }

        hb_msg = json.dumps(msg)
        worker_hbsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        worker_hbsock.connect(("localhost", self.master_port - 1))
        while not self.shutdown:
            print("Worker {} sending heartbeat".format(worker_pid))
            worker_hbsock.sendall(hb_msg.encode('utf-8'))
            time.sleep(2)
        worker_hbsock.close()

    def register(self, worker_port, worker_pid):
        """Send 'register' message from Worker to the Master."""
        register_dict = {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": worker_port,
            "worker_pid": worker_pid
        }

        message_json = json.dumps(register_dict)
        self.send_tcp_message(message_json)

        logging.debug(
            "Worker:%s received\n%s",
            worker_port,
            json.dumps(register_dict),
        )


def input_file_name(file_path):
    """Return only name of input file, given entire input path."""
    dirs = file_path.split("/")
    return dirs[-1]


@click.command()
@click.argument("master_port", nargs=1, type=int)
@click.argument("worker_port", nargs=1, type=int)
def main(master_port, worker_port):
    """Run main."""
    Worker(master_port, worker_port)


if __name__ == '__main__':
    main()
