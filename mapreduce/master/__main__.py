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
from queue import Queue
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
        # after actual tests start working, check the directories
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
        self.job_counter = 0
        self.job_queue = Queue()
        self.server_running = False
        #self.new_message = ""

        # TODO: create additional threads or setup you may need. (ex: fault tolerance)

        # Create a new TCP socket on the given port_number
        signals = {"shutdown": False, "first_worker": False}
        master_sock = tcp_socket(self.port)
        master_thread = threading.Thread(target=self.listen, args=(signals, master_sock,))
        master_thread.start()

        #run_thread = threading.Thread(target=self.run_jobs, args=(self.new_message))

        # FOR TESTING
        count = 0
        while not signals["shutdown"]:
            time.sleep(1)
            count += 1
            # TODO: as soon as a job finishes, start processing the next pending job
            # if not self.server_running: start next job
            if count > 15:
                break
    
        if signals["shutdown"]:
            self.send_shutdown()

            master_thread.join()
            self.heartbeat_thread.join()
            master_sock.close()


    def listen(self, signals, sock):
        """Wait on a message from a socket or a shutdown signal."""
        sock.settimeout(1)
        while not signals["shutdown"]:
            message_str = listen_setup(sock)

            if message_str == "":
                continue

            try:
                message_dict = json.loads(message_str)
                message_type = message_dict["message_type"]

                if message_type == "shutdown":
                    signals["shutdown"] = True

                elif message_type == "register":
                    self.register_worker(message_dict)
                    self.send_register_ack(message_dict)

                    if not signals["first_worker"]:
                        signals["first_worker"] = True
                        # TODO: if the master is already executing a map/group/reduce, it should assign the Worker the next available task immediately
                        # if self.server_running: assign this worker the next task

                elif message_type == "new_master_job":
                    self.new_master_job(message_dict) # this modifies message_dict. adds job_id key.

                elif message_type == "status":
                    self.worker_status(message_dict)

            except json.JSONDecodeError:
                continue


    def worker_status(self, message_dict):
        """Update state of worker."""
        pid = message_dict["worker_pid"]

        if message_dict['status'] == "finished":
            self.worker_threads[pid]['state'] = "ready"


    def new_master_job(self, message_dict):
        """Handle new_master_job message."""
        # create directories tmp/job-{id}. id is self.job_counter
        job_path = pathlib.Path("tmp/job-" + str(self.job_counter))
        job_path.mkdir()
        message_dict['job_id'] = self.job_counter # not sure if i'll regret this
        self.job_counter += 1

        mapper_path = pathlib.Path(job_path/"mapper-output")
        grouper_path = pathlib.Path(job_path/"grouper-output")
        reducer_path = pathlib.Path(job_path/"reducer-output")
        mapper_path.mkdir()
        grouper_path.mkdir()
        reducer_path.mkdir()

        # if MapReduce server is busy or no available workers, add job to queue
        if self.find_ready_worker() == -1 || self.server_running:
            self.job_queue.put(message_dict)
        else:
            # TODO: begin job execution
            self.server_running = True # TODO: when finished with job, set to False
            self.input_partitioning(message_dict)


    def input_partitioning(self, message_dict):
        """Parition the input files and distribute files to workers to start mapping."""
        # initialize list of num_mappers lists
        num_mappers = message_dicts['num_mappers']
        file_partitions = [[] for i in range(num_mappers)]

        job_id = message_dict['job_id']
        sorted_files = sorted(os.listdir(message_dict['input_directory']))

        # partition the files into num_mappers groups
        for index, file in enumerate(sorted_files):
            partition_idx = index % num_mappers
            file_partitions[partition_idx].append(message_dict['input_directory'] + "/" + file)

        # initial groups. workers in registration order
        ordered_pids = list(self.worker_threads)
        curr_map_idx = 0

        for pid in ordered_pids:
            self.worker_threads[pid]['state'] = "busy"
            job_dict = {
                "message_type": "new_worker_job",
                "input_files": file_partitions[curr_map_idx],
                "executable": message_dict['mapper_executable'],
                "output_directory": message_dict['output_directory'],
                "worker_pid": pid
            }
            job_json = json.dumps(job_dict)
            worker_port = self.worker_threads[pid]['worker_port']
            self.send_tcp_message(job_json, worker_port)

            curr_map_idx += 1

            if curr_map_idx == num_mappers:
                break

        # TODO: will have to handle getting worker status messages for mapping/grouping/reducing
        # maybe some signals like shutdown?
            
        # if more num_mappers than workers
        if num_mappers > len(ordered_pids):
            while curr_map_idx < num_mappers:
                ready_pid = self.find_ready_workers()
                while ready_pid == -1:
                    time.sleep(1)
                    ready_pid = self.find_ready_workers()

                job_dict = {
                    "message_type": "new_worker_job",
                    "input_files": file_partitions[curr_map_idx],
                    "executable": message_dict['mapper_executable'],
                    "output_directory": message_dict['output_directory'],
                    "worker_pid": ready_pid
                }
                job_json = json.dumps(job_dict)
                worker_port = self.worker_threads[ready_pid]['worker_port']
                self.send_tcp_message(job_json, worker_port)

                curr_map_idx += 1


    def find_ready_worker(self):
        """Return worker_pid of first available worker. If none available, return -1."""
        # keys (worker_pid) in registration order
        ordered_pids = list(self.worker_threads)

        for pid in ordered_pids:
            if self.worker_threads[pid]['state'] == 'ready':
                return pid

        return -1


    def register_worker(self, worker_message):
        """Add new worker to self.worker_threads dict."""
        self.worker_threads[worker_message['worker_pid']] = {
            "worker_host": worker_message['worker_host'],
            "worker_port": worker_message['worker_port'],
            "state": "ready",
        }


    def send_register_ack(self, worker_message):
        """Send register ack message to worker."""
        reg_response_dict = {
            "message_type": "register_ack",
            "worker_host": worker_message["worker_host"],
            "worker_port": worker_message["worker_port"],
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
        
        for worker in self.worker_threads.values():
            self.send_tcp_message(shutdown_json, worker['worker_port'])


    def send_tcp_message(self, message_json, worker_port):
        """Send a TCP message from the Master to a Worker."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            sock.connect(("localhost", worker_port))

            sock.sendall(message_json.encode('utf-8'))
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
