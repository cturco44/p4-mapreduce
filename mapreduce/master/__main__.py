import os
import logging
import json
import time
import click
import mapreduce.utils
import pathlib
import glob
import threading
import socket
from mapreduce.utils import listen_setup, tcp_socket


# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Master:
    def __init__(self, port):
        logging.info("Starting master:%s", port)
        logging.info("Master:%s PWD %s", port, os.getcwd())

        self.port = port

        # make new directory tmp if doesn't exist
        p = pathlib.Path("tmp/")
        if not os.path.isdir("tmp"): # /tmp or tmp or tmp/?
            p.mkdir()

        # delete any old job folders in tmp
        path_string = str(p/'job-*')
        jobPaths = glob.glob(path_string)
        # TODO: after actual tests start working, check the directories
        for path in jobPaths:
            try:
                os.rmdir(jobPaths)
            except:
                continue

        # create a new thread which will listen for udp heartbeats from workers (port - 1)
        # UDP socket for heartbeat
        heart_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        heart_sock.bind(("localhost", self.port - 1))
        
        self.heartbeat_thread = threading.Thread(target=self.heartbeat_listen, args=(heart_sock,))
        self.heartbeat_thread.start()

        self.worker_threads = {}

        # TODO: create additional threads or setup you may need. (ex: fault tolerance)

        # Create a new TCP socket on the given port_number and call the listen() function. 
        # Note: only one listen() thread should remain open for the whole lifetime of the master
        signals = {"shutdown": False, "first_worker": False}
        master_sock = tcp_socket(self.port)
        master_thread = threading.Thread(target=self.listen, args=(signals, master_sock,))
        master_thread.start()

        #if signals["first_worker"]:
            # TODO: check job queue to see if there is any work to assign to the Worker
            # if the master is already executing a map/group/reduce, it should assign the Worker the next available task immediately

        if signals["shutdown"]:
            self.send_shutdown()

        master_thread.join()
        self.heartbeat_thread.join() # should all threads join?


    def listen(self, signals, sock):
        """Wait on a message from a socket or a shutdown signal."""
        sock.settimeout(1)
        while not signals["shutdown"]:
            message_str = listen_setup(sock))

            try:
                message_dict = json.loads(message_str)
                message_type = message_dict["message_type"]

                if message_type == "shutdown":
                    signals["shutdown"] = True
                elif message_type == "register":
                    self.register_workers(message_dict)
                    self.send_register_ack(message_dict)

                    if not signals["first_worker"]:
                        signals["first_worker"] = True

            except json.JSONDecodeError:
                continue


    def register_worker(self, worker_message):
        """Add new worker to self.worker_threads dict."""
        self.worker_threads[worker_message['worker_pid']] = {
            "worker_host": worker_message['worker_host'],
            "worker_post": worker_message['worker_port'],
            "state": "ready",
        }


    def send_register_ack(self, worker_message):
        """Send register ack message to worker."""
        reg_response_dict = {
            "message_type": "register_ack",
            "worker_host": worker_message["worker_host"],
            "worker_post": worker_message["worker_port"],
            "worker_pid": worker_message["worker_pid"]
        }

        reg_json = json.dumps(reg_response_dict)

        self.send_tcp_message(reg_json, reg_response_dict["worker_port"])


    def send_shutdown(self):
        """Send shutdown messages to all workers."""
        shutdown_dict = {
            "message_type": "shutdown"
        }
        shutdown_json = json.dumps(shutdown_dict)
        
        for worker in self.worker_threads:
            self.send_tcp_message(shutdown_json, worker.worker_port)


    def send_tcp_message(self, message_json, worker_port):
        """Send a TCP message from the Master to a Worker."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            sock.connect(("localhost", worker_port))

            sock.sendall(str.encode(message_json))
            sock.close()
        except socket.error as err:
            print("Failed to send a message to a worker at port " + str(worker_port))
            print(err)


    def heartbeat_listen(self, sock):
        """Listens for UDP heartbeat messages from workers."""
        # TODO: heartbeat

        


@click.command()
@click.argument("port", nargs=1, type=int)
def main(port):
    Master(port)


if __name__ == '__main__':
    main()
