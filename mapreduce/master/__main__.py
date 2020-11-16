"""Master program."""
import os
import logging
import json
import time
import threading
import socket
import shutil
import pathlib
from queue import Queue
from heapq import merge
import click
from mapreduce.utils import listen_setup, tcp_socket


# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Master:
    """Master class."""

    def __init__(self, port):
        """Initialize Master class."""
        logging.info("Starting master:%s", port)
        logging.info("Master:%s PWD %s", port, os.getcwd())

        # make new directory tmp if doesn't exist
        # self.home = pathlib.Path.cwd()
        tmp = pathlib.Path.cwd() / "tmp"
        tmp.mkdir(exist_ok=True)

        # delete any old job folders in tmp
        job_paths = tmp.glob("job-*")
        # after actual tests start working, check the directories
        for path in job_paths:
            shutil.rmtree(path)

        self.shutdown = False
        self.worker_threads = {}
        self.busy_workers = {}
        self.job_counter = 0
        self.job_queue = Queue()
        self.dead_job_queue = Queue()  # store job_json of dead workers
        self.server_running = False

        # create new thread which will listen for heartbeats from workers
        # UDP socket for heartbeat
        heart_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        heart_sock.bind(("localhost", port - 1))

        check_queue_thread = threading.Thread(target=self.check_queue)
        check_queue_thread.start()

        fault_tol_thread = threading.Thread(target=self.fault_tolerance)
        fault_tol_thread.start()

        heartbeat_thread = threading.Thread(
            target=self.heartbeat_listen, args=(heart_sock,)
        )
        heartbeat_thread.start()

        # Create a new TCP socket on the given port_number

        master_sock = tcp_socket(port)
        master_thread = threading.Thread(target=self.listen,
                                         args=(master_sock,))
        master_thread.start()
        heartbeat_thread.join()
        print("a")
        check_queue_thread.join()
        print("b")
        fault_tol_thread.join()
        print("c")
        master_thread.join()
        print("d")

        master_sock.close()
        print("Master finish shutdown")

    def listen(self, sock):
        """Wait on a message from a socket or a shutdown signal."""
        sock.settimeout(1)
        while not self.shutdown:
            message_str = listen_setup(sock)

            if message_str == "":
                continue

            try:
                message_dict = json.loads(message_str)
                message_type = message_dict["message_type"]
                print("Master recv msg: ", json.dumps(message_dict))

                if message_type == "shutdown":
                    self.send_shutdown()  # got shutdown signal
                    self.shutdown = True

                elif message_type == "register":
                    self.register_worker(message_dict)
                    send_register_ack(message_dict)

                elif message_type == "new_master_job":
                    self.new_master_job(message_dict)
                    self.job_queue.put(message_dict)

                elif message_type == "status":
                    self.worker_status(message_dict)

            except json.JSONDecodeError:
                continue
            except socket.timeout:
                continue

    def worker_status(self, message_dict):
        """Update state of worker."""
        pid = message_dict["worker_pid"]

        if message_dict["status"] == "finished":
            self.worker_threads[pid]["state"] = "ready"
            self.busy_workers.pop(pid)

    def new_master_job(self, message_dict):
        """Handle new_master_job message."""
        # create directories tmp/job-{id}. id is self.job_counter
        home_dir_tmp = pathlib.Path.cwd() / "tmp"
        job_path = home_dir_tmp / "job-{}".format(str(self.job_counter))
        print("job_path: ", job_path)
        job_path.mkdir(parents=True)
        message_dict["job_id"] = self.job_counter
        self.job_counter += 1

        mapper_path = job_path / "mapper-output"
        grouper_path = job_path / "grouper-output"
        reducer_path = job_path / "reducer-output"
        mapper_path.mkdir(parents=True)
        grouper_path.mkdir(parents=True)
        reducer_path.mkdir(parents=True)

    def execute_task(self, message_dict):
        """Execute task."""
        # begin job execution
        self.server_running = True
        self.mapreduce(message_dict, "map")
        self.group(message_dict)
        # pdb.set_trace()
        self.mapreduce(message_dict, "reduce")
        wrap_up(message_dict)
        self.server_running = False

    def check_queue(self):
        """Check queue."""
        while not self.shutdown:
            if not self.job_queue.empty():
                if not self.server_running and self.find_ready_worker() != -1:
                    message_dict = self.job_queue.get()
                    self.execute_task(message_dict)
            # time.sleep(1)

    def group(self, message_dict):
        """Group files and make directories."""
        num_workers = self.get_num_available_workers()

        file_partitions = [[] for _ in range(num_workers)]

        home_dir_tmp = pathlib.Path.cwd() / "tmp"
        job_dir = home_dir_tmp / "job-{}".format(str(message_dict["job_id"]))

        input_files = [
            str(file.relative_to(pathlib.Path.cwd()))
            for file in pathlib.Path(message_dict["input_directory"]).iterdir()
            if file.is_file()
        ]  # files are paths

        # partition the files into num_mappers groups
        for index, file in enumerate(sorted(input_files)):
            partition_idx = index % num_workers
            file_partitions[partition_idx].append(file)

        cur_work_idx = 0
        output_file_number = 1
        while cur_work_idx < num_workers and not self.shutdown:
            print("num_workers {}".format(num_workers))
            print("group running")
            ready_worker_id = -1
            while ready_worker_id == -1:
                # time.sleep(1)
                if not self.shutdown:
                    ready_worker_id = self.find_ready_worker()
                    if ready_worker_id != -1:
                        print("sort worker found {} ".format(ready_worker_id))
                else:
                    break
            output_file = job_dir / "grouper-output" / \
                ("sorted" + format_no(output_file_number))
            job_dict = {
                "message_type": "new_sort_job",
                "input_files": file_partitions[cur_work_idx],
                "output_file": str(output_file.
                                   relative_to(pathlib.Path.cwd())),
                "worker_pid": ready_worker_id,
            }
            output_file_number += 1
            self.worker_threads[ready_worker_id]["state"] = "busy"
            self.busy_workers[ready_worker_id] = json.dumps(job_dict)
            send_tcp_message(json.dumps(job_dict),
                             self.worker_threads[ready_worker_id]
                             ["worker_port"])

            cur_work_idx += 1
        # modify input dir for next phase after all jobs for this phase is done
        message_dict["input_directory"] = str(job_dir / "grouper-output")
        while self.busy_workers:
            # time.sleep(1) ?
            if self.shutdown:
                break
            continue
        print("sorting finished")

        group_helper(message_dict)

    def get_num_available_workers(self):
        """Get num available workers."""
        available = 0
        for key in self.worker_threads:
            if self.worker_threads[key]["state"] == "ready":
                available = available + 1
        return available

    def find_ready_worker(self):
        """Return pid of first ready worker. If none, return -1."""
        # keys (worker_pid) in registration order
        ordered_pids = list(self.worker_threads)

        for pid in ordered_pids:
            if self.worker_threads[pid]["state"] == "ready":
                return pid

        return -1

    def mapreduce(self, message_dict, job_type):
        """Reduce and map execution."""
        # num_workers is number required in job, not total num of workers
        num_workers = (
            message_dict["num_mappers"]
            if job_type == "map"
            else message_dict["num_reducers"]
        )
        file_partitions = [[] for _ in range(num_workers)]

        job_dir = (pathlib.Path.cwd() / "tmp" /
                   "job-{}".format(str(message_dict["job_id"])))

        input_files = mapreduce_helper(job_type, message_dict)

        tmp_output = "mapper-output" if job_type == "map" else "reducer-output"
        # output_dir = job_dir / tmp_output
        # partition the files into num_mappers groups
        for index, file in enumerate(sorted(input_files)):
            partition_idx = index % num_workers
            file_partitions[partition_idx].append(file)

        cur_work_idx = 0

        executable_type = (
            "mapper_executable" if job_type == "map" else "reducer_executable"
        )

        while cur_work_idx < num_workers and not self.shutdown:
            ready_worker_id = -1
            while ready_worker_id == -1:
                # time.sleep(1)
                if not self.shutdown:
                    ready_worker_id = self.find_ready_worker()
                else:
                    break
            print("sending message to worker {}".format(ready_worker_id))
            job_dict = {
                "message_type": "new_worker_job",
                "input_files": file_partitions[cur_work_idx],
                "executable": message_dict[executable_type],
                "output_directory": str((job_dir / tmp_output).
                                        relative_to(pathlib.Path.cwd())),
                "worker_pid": ready_worker_id,
            }
            self.worker_threads[ready_worker_id]["state"] = "busy"
            self.busy_workers[ready_worker_id] = json.dumps(job_dict)
            send_tcp_message(json.dumps(job_dict),
                             self.worker_threads[ready_worker_id]
                             ["worker_port"])

            cur_work_idx += 1
        # modify input dir for next phase after all jobs for this phase is done
        message_dict["input_directory"] = str(job_dir / tmp_output)
        while self.busy_workers:
            # time.sleep(1) ?
            if self.shutdown:
                break
            continue
        # if while loop ends, this means all work needed for this job is done

    def fault_tolerance(self):
        """Check fault tolerance."""
        while not self.shutdown:

            if not self.dead_job_queue.empty():
                print("fault running")
                job_json = self.dead_job_queue.get()
                temp_dict = json.loads(job_json)
                ready_worker_id = -1
                while ready_worker_id == -1:
                    if self.shutdown:
                        break
                    # time.sleep(1)
                    ready_worker_id = self.find_ready_worker()
                self.worker_threads[ready_worker_id]["state"] = "busy"
                temp_dict["worker_pid"] = ready_worker_id
                job_json = json.dumps(temp_dict)
                self.busy_workers[ready_worker_id] = job_json
                print("sending dead job to {}".format(ready_worker_id))
                print("\n")
                print(job_json)
                worker_port = (self.worker_threads[ready_worker_id]
                               ["worker_port"])
                send_tcp_message(job_json, worker_port)

    def register_worker(self, worker_message):
        """Add new worker to self.worker_threads dict."""
        self.worker_threads[worker_message["worker_pid"]] = {
            "worker_host": worker_message["worker_host"],
            "worker_port": worker_message["worker_port"],
            "state": "ready",
            "last_seen": time.time(),
        }

    def send_shutdown(self):
        """Send shutdown messages to all workers."""
        shutdown_dict = {"message_type": "shutdown"}
        shutdown_json = json.dumps(shutdown_dict)

        for worker in self.worker_threads.values():
            if worker["state"] != "dead":
                send_tcp_message(shutdown_json, worker["worker_port"])

    def heartbeat_listen(self, sock):
        """Listen for UDP heartbeat messages from workers."""
        sock.settimeout(1)
        while not self.shutdown:
            try:
                data = sock.recv(4096)  # data is a byte object
                cur_time = time.time()
                msg = json.loads(data.decode())
                print("master receive hb from {}".format(msg["worker_pid"]))
                # ignore not registered worker heartbeat
                if msg["worker_pid"] in self.worker_threads:
                    self.heartbeat_helper(cur_time, msg)
            except json.JSONDecodeError:
                continue
            except socket.timeout:
                continue
        sock.close()

    def heartbeat_helper(self, cur_time, msg):
        """Is helper function for heartbeat_listen()."""
        for worker_pid, info in self.worker_threads.items():
            if cur_time - info["last_seen"] >= 10.0:
                if worker_pid in self.busy_workers:
                    self.dead_job_queue.put(self.busy_workers[worker_pid])
                    self.busy_workers.pop(worker_pid)
                self.worker_threads[worker_pid]["state"] = "dead"
                print("killed{}".format(worker_pid))
            if worker_pid == msg["worker_pid"]:
                if self.worker_threads[worker_pid]["state"] != "dead":
                    self.worker_threads[worker_pid]["last_seen"] = cur_time


def send_tcp_message(message_json, worker_port):
    """Send a TCP message from the Master to a Worker."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        sock.connect(("localhost", worker_port))

        sock.sendall(message_json.encode("utf-8"))
        sock.close()
    except socket.error as err:
        print("Failed to send a message to a worker at port "
              + str(worker_port))
        print(err)


def wrap_up(message_dict):
    """Wrap up."""
    input_dir = message_dict["input_directory"]
    output_dir = pathlib.Path(message_dict["output_directory"])
    output_dir.mkdir(parents=True, exist_ok=True)

    for path in pathlib.Path(input_dir).iterdir():
        num = str(path.stem)[6:]
        outputfile = "outputfile" + num
        # renaming moves the file to new location
        path.rename(output_dir / outputfile)


def send_register_ack(worker_message):
    """Send register ack message to worker."""
    reg_response_dict = {
        "message_type": "register_ack",
        "worker_host": worker_message["worker_host"],
        "worker_port": worker_message["worker_port"],
        "worker_pid": worker_message["worker_pid"],
    }

    reg_json = json.dumps(reg_response_dict)
    print("Master registering worker {}".format(worker_message["worker_port"]))
    send_tcp_message(reg_json, reg_response_dict["worker_port"])


def format_no(number):
    """Format no."""
    if number < 10:
        return "0" + str(number)
    return str(number)


def create_reducer_files(message_dict):
    """Create new files for reducing in group_helper()."""
    reducer_files = []
    input_dir = pathlib.Path(message_dict["input_directory"])
    for i in range(message_dict["num_reducers"]):
        reducer_path = input_dir / ("reduce" + format_no(i + 1))
        reducer_path.touch(exist_ok=True)
        file_new = open(str(reducer_path), "w")
        reducer_files.append(file_new)

    return reducer_files


def group_helper(message_dict):
    """Is helper function for group()."""
    in_dir = pathlib.Path(message_dict["input_directory"])
    starter_files = [str(file) for file in in_dir.iterdir() if file.is_file()]
    file_list = []
    for grouper_file in starter_files:
        file_new = open(grouper_file, "r")
        file_list.append(file_new)
    it_files = merge(*file_list)

    # create new files
    reducer_files = create_reducer_files(message_dict)

    num_reducers = message_dict["num_reducers"]
    key_num = 0
    old_key = ""
    num_loops = 0
    while True:
        try:
            item = next(it_files)
            split = item.split("\t")
            key = split[0]
            if (num_loops != 0) and (old_key != key):
                old_key = key
                key_num = key_num + 1
            reducer_files[key_num % num_reducers].write(item)
            num_loops = num_loops + 1

        except StopIteration:
            break
    # close all files
    for item in reducer_files:
        item.close()
    for item in file_list:
        item.close()


def mapreduce_helper(job_type, message_dict):
    """Is helper for mapreduce()."""
    input_dir = pathlib.Path(message_dict["input_directory"])
    if job_type == "map":
        input_files = [
            str(file) for file in input_dir.iterdir() if file.is_file()
        ]  # files are paths
    else:
        input_files = [
            str(file.relative_to(pathlib.Path.cwd()))
            for file in input_dir.glob("reduce*")
            if file.is_file()
        ]

    return input_files


@click.command()
@click.argument("port", nargs=1, type=int)
def main(port):
    """Run main."""
    Master(port)


if __name__ == "__main__":
    main()
