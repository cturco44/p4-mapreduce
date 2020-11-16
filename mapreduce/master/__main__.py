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
from heapq import merge


# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Master:
    def __init__(self, port):
        logging.info("Starting master:%s", port)
        logging.info("Master:%s PWD %s", port, os.getcwd())

        self.port = port

        # make new directory tmp if doesn't exist
        tmp = pathlib.Path.cwd() / "tmp"
        tmp.mkdir(exist_ok=True)
        self.tmp = tmp

        # delete any old job folders in tmp
        jobPaths = self.tmp.glob('job-*')
        # after actual tests start working, check the directories
        for path in jobPaths:
            shutil.rmtree(path)

        self.shutdown = False
        self.worker_threads = {}
        self.busy_workers = {}
        self.job_counter = 0
        self.job_queue = Queue()
        self.dead_job_queue = Queue() #store job_json of dead workers
        self.server_running = False

        # create a new thread which will listen for udp heartbeats from workers (port - 1)
        # UDP socket for heartbeat
        heart_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        heart_sock.bind(("localhost", self.port - 1))

        check_queue_thread = threading.Thread(target=self.check_queue)
        check_queue_thread.start()

        fault_tol_thread = threading.Thread(target=self.fault_tolerance)
        fault_tol_thread.start()

        heartbeat_thread = threading.Thread(target=self.heartbeat_listen, args=(heart_sock,))
        heartbeat_thread.start()



        # Create a new TCP socket on the given port_number

        master_sock = tcp_socket(self.port)
        master_thread = threading.Thread(target=self.listen, args=(master_sock,))
        master_thread.start()


        # TODO: THINGS TO KEEP TRACK OF
        # ITERATE JOB_COUNTER WHEN DONE WITH JOB (AFTER REDUCING)
        # SET SERVER_RUNNING TO FALSE WHEN DONE WITH JOB (AFTER REDUCING)

        '''
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
        '''
        #print("Master :",len(threading.enumerate()))
        #print(threading.enumerate())
        #print(self.dead_job_queue.empty())
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
                # TODO: for testing
                print("Master recv msg: ", json.dumps(message_dict, indent=2))

                if message_type == "shutdown":
                    self.shutdown = True
                    self.send_shutdown() #got shutdown signal


                elif message_type == "register":
                    self.register_worker(message_dict)
                    self.send_register_ack(message_dict)

                elif message_type == "new_master_job":
                    self.new_master_job(message_dict)
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
            self.busy_workers.pop(pid)


    def new_master_job(self, message_dict):
        """Handle new_master_job message."""
        # create directories tmp/job-{id}. id is self.job_counter
        job_path = self.tmp / "job-{}".format(str(self.job_counter))
        print("job_path: ", job_path)
        job_path.mkdir(parents=True)
        message_dict['job_id'] = self.job_counter
        self.job_counter += 1

        mapper_path = job_path / "mapper-output"
        grouper_path = job_path / "grouper-output"
        reducer_path = job_path / "reducer-output"
        mapper_path.mkdir(parents=True)
        grouper_path.mkdir(parents=True)
        reducer_path.mkdir(parents=True)



    def execute_task(self, message_dict):
        # begin job execution
        self.server_running = True
        self.mapreduce(message_dict, "map")
        self.group(message_dict)
        self.mapreduce(message_dict, "reduce")
        self.wrap_up(message_dict)
        self.server_running = False

    def find_ready_worker(self):
        """Return worker_pid of first available worker. If none available, return -1."""
        # keys (worker_pid) in registration order
        ordered_pids = list(self.worker_threads)

        for pid in ordered_pids:
            if self.worker_threads[pid]['state'] == 'ready':
                return pid

        return -1
    def check_queue(self):
        while not self.shutdown:
            if not self.job_queue.empty():
                if not self.server_running and self.find_ready_worker() != -1:
                    message_dict = self.job_queue.get()
                    self.execute_task(message_dict)
            #time.sleep(1)
    def group(self, message_dict):
        """Group files and then """
        # num_workers is number required in job, not total num of workers
        num_workers = len(self.worker_threads)

        file_partitions = [[] for _ in range(num_workers)]

        job_id = message_dict['job_id']
        job_dir = self.tmp / "job-{}".format(str(job_id))

        input_dir = pathlib.Path(message_dict["input_directory"])
        input_files = [str(file) for file in input_dir.iterdir() if file.is_file()] #files are paths
        sorted_files = sorted(input_files)

        output_dir = job_dir / "grouper-output"
        # partition the files into num_mappers groups
        for index, file in enumerate(sorted_files):
            partition_idx = index % num_workers
            file_partitions[partition_idx].append(file)

        cur_work_idx = 0
        output_file_number = 1
        while cur_work_idx < num_workers:
            ready_worker_id = -1
            while ready_worker_id == -1:
                #time.sleep(1)
                ready_worker_id = self.find_ready_worker()
               
            job_dict = {
                "message_type": "new_sort_job",
                "input_files": file_partitions[cur_work_idx],
                "output_file": str(output_dir / ("sorted" + self.format_no(output_file_number))),
                "worker_pid": ready_worker_id
            }
            output_file_number += 1
            job_json = json.dumps(job_dict, indent=2)
            self.worker_threads[ready_worker_id]['state'] = "busy"
            self.busy_workers[ready_worker_id] = job_json
            worker_port = self.worker_threads[ready_worker_id]['worker_port']
            self.send_tcp_message(job_json, worker_port)

            cur_work_idx += 1
        #modify input directory for next phase after all jobs for this phase is done
        message_dict['input_directory'] = str(output_dir)
        while self.busy_workers:
            #time.sleep(1) ?
            continue
        
        
        input_dir = pathlib.Path(message_dict["input_directory"])
        starter_files = [str(file) for file in input_dir.iterdir() if file.is_file()]
        file_list = []
        for grouper_file in starter_files:
            f = open(grouper_file, "r")
            file_list.append(f)
        it = merge(*file_list)
        
        #create new files
        reducer_files = []
        for i in range(message_dict['num_reducers']):
            reducer_path = input_dir / ("reduce" + self.format_no(i + 1))
            reducer_path.touch(exist_ok=True)
            f = open(str(reducer_path), 'w')
            reducer_files.append(f)
        
        num_reducers = message_dict['num_reducers']
        key_num = 0
        old_key = ""
        num_loops = 0
        while True: 
            try: 
                item = next(it)
                split = item.split("\t")
                key = split[0]
                if(num_loops != 0) and (old_key != key):
                    old_key = key
                    key_num = key_num + 1
                reducer_files[key_num % num_reducers].write(item)
                num_loops = num_loops + 1
                
            except StopIteration: 
                break
        #close all files
        for item in reducer_files:
            item.close()
        for item in file_list:
            item.close()
        #delete old files
        for p in input_dir.glob("sorted*"):
            p.unlink()

    def format_no(self, number):
        if number < 10:
            return '0' + str(number)
        return str(number)


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

        executable_type = "mapper_executable" if job_type == "map" else "reducer_executable"

        while cur_work_idx < num_workers:
            ready_worker_id = -1
            while ready_worker_id == -1:
                #time.sleep(1)
                ready_worker_id = self.find_ready_worker()

            job_dict = {
                "message_type": "new_worker_job",
                "input_files": file_partitions[cur_work_idx],
                "executable": message_dict[executable_type],
                "output_directory": str(output_dir),
                "worker_pid": ready_worker_id
            }
            job_json = json.dumps(job_dict, indent=2)
            self.worker_threads[ready_worker_id]['state'] = "busy"
            self.busy_workers[ready_worker_id] = job_json
            worker_port = self.worker_threads[ready_worker_id]['worker_port']
            self.send_tcp_message(job_json, worker_port)

            cur_work_idx += 1
        #modify input directory for next phase after all jobs for this phase is done
        message_dict['input_directory'] = str(output_dir)
        while self.busy_workers:
            #time.sleep(1) ?
            continue
        # if while loop ends, this means all work needed for this job is done

    def wrap_up(self, message_dict):
        input_dir = message_dict['input_directory']
        output_dir = pathlib.Path(message_dict['output_directory'])
        output_dir.mkdir(parents=True, exist_ok=True)

        for path in pathlib.Path(input_dir).iterdir():
            num = str(path.stem)[6:]
            outputfile = "outputfile" + num
            path.rename(output_dir / outputfile) # renaming moves the file to new location

    def fault_tolerance(self):
        while not self.shutdown:
            #print("fault running")
            if not self.dead_job_queue.empty():
                job_json = self.dead_job_queue.get()
                ready_worker_id = -1
                while ready_worker_id == -1:
                    if self.shutdown:
                        break
                    #time.sleep(1)
                    ready_worker_id = self.find_ready_worker()
                self.worker_threads[ready_worker_id]['state'] = "busy"
                self.busy_workers[ready_worker_id] = job_json
                worker_port = self.worker_threads[ready_worker_id]['worker_port']
                self.send_tcp_message(job_json, worker_port)

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

        reg_json = json.dumps(reg_response_dict, indent=2)

        self.send_tcp_message(reg_json, reg_response_dict["worker_port"])


    def send_shutdown(self):
        """Send shutdown messages to all workers."""
        shutdown_dict = {
            "message_type": "shutdown"
        }
        shutdown_json = json.dumps(shutdown_dict, indent=2)

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
        sock.settimeout(1)

        while not self.shutdown:
            try:
                data = sock.recv(4096) # data is a byte object
                cur_time = time.time()
                msg = json.loads(data.decode())
                if msg['worker_pid'] in self.worker_threads: #ignore not registered worker heartbeat
                    for worker_pid, info in self.worker_threads.items():
                        if cur_time - info["last_seen"] >= 10.0:
                            if worker_pid in self.busy_workers:
                                self.dead_job_queue.put(self.busy_workers[worker_pid])
                            self.worker_threads[worker_pid]["state"] = "dead"
                        if worker_pid == msg["worker_pid"]:
                            if self.worker_threads[worker_pid]["state"] != "dead":
                                self.worker_threads[worker_pid]["last_seen"] = cur_time
            except json.JSONDecodeError:
                continue
            except socket.timeout:
                continue
        sock.close()

@click.command()
@click.argument("port", nargs=1, type=int)
def main(port):
    Master(port)


if __name__ == '__main__':
    main()
