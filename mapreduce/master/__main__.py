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
import sys
import shutil
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
        cwd = pathlib.Path.cwd()
        tmp = cwd.parent.parent / "tmp"
        tmp.mkdir(exist_ok=True)
        self.tmp = tmp

        # delete any old job folders in tmp
        jobPaths = tmp.glob('job-*')
        # after actual tests start working, check the directories
        for path in jobPaths:
            shutil.rmtree(path)

        # create a new thread which will listen for udp heartbeats from workers (port - 1)
        # UDP socket for heartbeat
        heart_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        heart_sock.bind(("localhost", self.port - 1))

        self.heartbeat_thread = threading.Thread(target=self.heartbeat_listen, args=(heart_sock,))
        self.heartbeat_thread.start()

        self.worker_threads = {}
        self.busy_workers = 0
        self.job_counter = 0
        self.job_queue = Queue()
        self.server_running = False

        # TODO: create additional threads or setup you may need. (ex: fault tolerance)

        # TODO: not sure if listen and actually doing the tasks (input partitioning, mapping, grouping, reducing) should be in
        # two separate threads?

        check_queue_thread = threading.Thread(target=self.check_queue, args=(self.job_queue,))
        check_queue_thread.start()
        # Create a new TCP socket on the given port_number
        signals = {"shutdown": False, "first_worker": False}
        master_sock = tcp_socket(self.port)
        master_thread = threading.Thread(target=self.listen, args=(signals, master_sock,))
        master_thread.start()


        # TODO: THINGS TO KEEP TRACK OF
        # ITERATE JOB_COUNTER WHEN DONE WITH JOB (AFTER REDUCING)
        # SET SERVER_RUNNING TO FALSE WHEN DONE WITH JOB (AFTER REDUCING)

        # TO WHOEVER'S DOING GROUPING, CALL THE GROUPING FUNCTION AT THE END OF INPUT_PARTIONING()

        # FOR TESTING
        count = 0
        while not signals["shutdown"]:
            time.sleep(1)
            count += 1
            # TODO: as soon as a job finishes, start processing the next pending job
            # if not self.server_running: start next job
            if count > 8:
                signals["shutdown"] = True
                break

        self.send_shutdown() # here or in listen()?
        # check_queue_thread.join ? daemon?
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
                # TODO: for testing
                print(message_dict)

                if message_type == "shutdown":
                    signals["shutdown"] = True
                    break

                elif message_type == "register":
                    self.register_worker(message_dict)
                    self.send_register_ack(message_dict)

                    if not signals["first_worker"]:
                        signals["first_worker"] = True
                        # TODO: if the master is already executing a map/group/reduce, it should assign the Worker the next available task immediately
                        # if self.server_running: assign this worker the next task
                        # taken care of by check_queue?
                elif message_type == "new_master_job":
                    self.job_queue.put(message_dict)

                elif message_type == "status":
                    self.worker_status(message_dict)

            except json.JSONDecodeError:
                continue


    def worker_status(self, message_dict):
        """Update state of worker."""
        pid = message_dict["worker_pid"]

        if message_dict['status'] == "finished":
            self.worker_threads[pid]['state'] = "ready"
            self.busy_workers -= 1


    def new_master_job(self, message_dict):
        """Handle new_master_job message."""
        # create directories tmp/job-{id}. id is self.job_counter
        job_path = self.tmp / "job-{}".format(str(self.job_counter))
        job_path.mkdir(parents=True)
        message_dict['job_id'] = self.job_counter
        self.job_counter += 1

        mapper_path = job_path / "mapper-output"
        grouper_path = job_path / "grouper-output"
        reducer_path = job_path / "reducer-output"
        mapper_path.mkdir(parents=True)
        grouper_path.mkdir(parents=True)
        reducer_path.mkdir(parents=True)


        # begin job execution
        self.server_running = True
        self.mapreduce(message_dict, "map")
        #TODO Group function
        self.mapreduce(message_dict, "reduce")
        self.wrap_up(message_dict)
        self.server_running = False
        #self.input_partitioning(message_dict)

    '''
    def input_partitioning(self, message_dict):
        """Parition the input files and distribute files to workers to do mapping."""
        # initialize list of num_mappers lists
        num_mappers = message_dict['num_mappers']
        file_partitions = [[] for i in range(num_mappers)]

        job_id = message_dict['job_id']
        input_dir = pathlib.Path(message_dict["input_directory"])
        input_files = [str(file) for file in input_dir.glob('*') if file.is_file()] #files are paths
        sorted_files = sorted(input_files)

        # partition the files into num_mappers groups
        for index, file in enumerate(sorted_files):
            partition_idx = index % num_mappers
            file_partitions[partition_idx].append(file)

        # initial groups. workers in registration order
        ordered_pids = list(self.worker_threads)
        curr_map_idx = 0

        for pid in ordered_pids:
            self.worker_threads[pid]['state'] = "busy"
            output_dir = self.tmp / job_id / "mapper-output"
            job_dict = {
                "message_type": "new_worker_job",
                "input_files": file_partitions[curr_map_idx],
                "executable": message_dict['mapper_executable'],
                "output_directory": str(ouput_dir),
                "worker_pid": pid
            }
            job_json = json.dumps(job_dict)
            worker_port = self.worker_threads[pid]['worker_port']
            self.send_tcp_message(job_json, worker_port)

            curr_map_idx += 1

            if curr_map_idx == num_mappers:
                break

        # if more num_mappers than workers
        if num_mappers > len(ordered_pids):
            while curr_map_idx < num_mappers:
                ready_pid = self.find_ready_worker()
                # look for ready workers
                while ready_pid == -1:
                    time.sleep(1)
                    ready_pid = self.find_ready_worker()

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

        # TODO: GROUPING
    '''

    def find_ready_worker(self):
        """Return worker_pid of first available worker. If none available, return -1."""
        # keys (worker_pid) in registration order
        ordered_pids = list(self.worker_threads)

        for pid in ordered_pids:
            if self.worker_threads[pid]['state'] == 'ready':
                return pid

        return -1

    def mapreduce(self, message_dict, job_type):
        # num_workers is number required in job, not total num of workers
        num_workers = message_dict['num_mappers'] if job_type == "map" else message_dict["num_reducers"]

        file_partitions = [[] for _ in range(num_workers)]

        job_id = message_dict['job_id']
        job_dir = self.tmp / "job-{}".format(str(job_id))

        input_dir = pathlib.Path(message_dict["input_directory"])
        input_files = [str(file) for file in input_dir.iterdir() if file.is_file()] #files are paths
        sorted_files = sorted(input_files)

        tmp_output = "mapper-output" if job_type == "map" else "reducer-output"
        output_dir = job_dir / tmp_output
        # partition the files into num_mappers groups
        for index, file in enumerate(sorted_files):
            partition_idx = index % num_workers
            file_partitions[partition_idx].append(file)

        cur_work_idx = 0

        executable_type = "mapper-executable" if job_type == "map" else "reducer-executable"

        while cur_work_idx < num_workers:
            ready_worker_id = -1
            while ready_worker_id == -1
                time.sleep(1)
                ready_worker_id = self.find_ready_worker()

            job_dict = {
                "message_type": "new_worker_job",
                "input_files": file_partitions[cur_work_idx],
                "executable": message_dict[executable_type],
                "output_directory": str(output_dir),
                "worker_pid": ready_worker_id
            }
            job_json = json.dumps(job_dict)
            self.worker_threads[ready_worker_id]['state'] = "busy"
            self.busy_workers += 1
            worker_port = self.worker_threads[ready_worker_id]['worker_port']
            self.send_tcp_message(job_json, worker_port)

            cur_work_idx += 1
        #modify input directory for next phase after all jobs for this phase is done
        message_dict['input_directory'] = str(output_dir)
        while self.busy_workers != 0:
            time.sleep(1)
        # if while loop ends, this means all work needed for this job is done

    def wrap_up(self, message_dict):
        input_dir = message_dict['input_directory']
        output_dir = pathlib.Path(message_dict['output_directory'])
        output_dir.mkdir(parents=True, exist_ok=True)

        for path in pathlib.Path(input_dir).iterdir():
            num = str(path.stem)[6:]
            outputfile = "outputfile" + num
            path.rename(output_dir / outputfile) # renaming moves the file to new location
    def register_worker(self, worker_message):
        """Add new worker to self.worker_threads dict."""
        self.worker_threads[worker_message['worker_pid']] = {
            "worker_host": worker_message['worker_host'],
            "worker_port": worker_message['worker_port'],
            "state": "ready",
            "last_seen": time.time()
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

    def check_queue(self, queue):
        while True:
            if queue:
                if self.find_ready_worker() != -1 and not self.server_running:
                    message_dict = queue.get()
                    job_thread = threading.Thread(target=self.new_master_job, args=(message_dict,))
                    job_thread.start()
                    job_thread.join()
    def heartbeat_listen(self, sock):
        """Listens for UDP heartbeat messages from workers."""
        sock.settimeout(10) # 2s * 5 pings = 10s

        while True:
            try:
                data, addr = sock.recvfrom() # data is a byte object, addr is (host, port)
                cur_time = time.time()
                msg = json.loads(data.decode())

                for worker_pid, info in self.worker_threads.items():
                    if cur_time - info["last_seen"] >= 10.0:
                        self.worker_threads[worker_pid]["status"] = "dead"
                    if worker_pid == msg["worker_pid"]:
                        if self.worker_threads[worker_pid]["status"] != "dead":
                            self.worker_threads[worker_pid]["last_seen"] = cur_time
                # TODO reassign job if not complete
            except json.JSONDecodeError:
                continue
            except socket.timeout: #all workers dead
                for id, worker in self.worker_threads.items():
                    worker["state"] = "dead"
                continue

@click.command()
@click.argument("port", nargs=1, type=int)
def main(port):
    Master(port)


if __name__ == '__main__':
    main()
