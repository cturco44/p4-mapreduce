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


# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Master:
    def __init__(self, port):
        logging.info("Starting master:%s", port)
        logging.info("Master:%s PWD %s", port, os.getcwd())

        # make new directory tmp if doesn't exist
        if not os.path.isdir("tmp"): # /tmp or tmp or tmp/?
            p = pathlib.Path("tmp/")
            p.mkdir()

        # delete any old job folders in tmp
        jobPaths = glob.glob(p/'job-*')

        for path in jobPaths:
            try:
                os.rmdir(jobPaths)

        # create a new thread which will listen for udp heartbeats from workers (port - 1)
        heartbeat_thread = threading.Thread(target=listen, args=(signals,))
        heartbeat_thread.start()

        # TODO: create additional threads or setup you may need. (ex: fault tolerance)
        

        # TODO: you should remove this. This is just so the program doesn't
        # exit immediately!
        logging.debug("IMPLEMENT ME!")
        time.sleep(120)


    def listen(signals):
        """Wait on a message from a socket or a shutdown signal."""
        # create a new tcp socket on given port and call listen() function. only one listen() thread.
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # bind socket to server
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(port)
        sock.listen()

        # in example...
        sock.settimeout(1)

        while not signals["shutdown"]:
            # listen for a connection
            try:
                clientsocket, address = sock.accept()
            except socket.timeout:
                continue

        message_chunks = []
        while True:
            try:
                data = clientsocket.recv(4096) # idK??
            except socket.timeout:
                continue
            if not data:
                break
            message_chunks.append(data)
        clientsocket.close()

        message_bytes = b''.join(message_chunks)
        message_str = message_bytes.decode('utf-8')

        try:
            message_dict = json.loads(message_str)
        except JSONDecodeError:
            continue

        logging.debug("Master:%s received\n%s",
            port,
            json.dumps(message_dict, indent=2),
        )





@click.command()
@click.argument("port", nargs=1, type=int)
def main(port):
    Master(port)


if __name__ == '__main__':
    main()
