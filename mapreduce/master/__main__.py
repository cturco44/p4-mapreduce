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
from mapreduce.utils import listen, create_socket


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
        signals = {"shutdown": False}
        sock = create_socket(port)
        heartbeat_thread = threading.Thread(target=listen, args=(signals, sock,))
        heartbeat_thread.start()

        # TODO: create additional threads or setup you may need. (ex: fault tolerance)

        # TODO: shutdown
        


@click.command()
@click.argument("port", nargs=1, type=int)
def main(port):
    Master(port)


if __name__ == '__main__':
    main()
