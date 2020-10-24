"""Example of waiting on a socket or a shutdown signal."""
import threading
import time
import socket
import json


def main():
    """Main thread, which spawns a second listen() thread."""
    print("main() starting")
    signals = {"shutdown": False}
    thread = threading.Thread(target=listen, args=(signals,))
    thread.start()
    time.sleep(10) # This gives up execution to the 'listen' thread
    signals["shutdown"] = True  # Tell listen thread to shut down
    thread.join()  # Wait for listen thread to shut down
    print("main() shutting down")


def listen(signals):
    """Wait on a message from a socket OR a shutdown signal."""
    print("listen() starting")

    # Create an INET, STREAMing socket, this is TCP
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Bind the socket to the server
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("localhost", 8000))
    sock.listen()

    # Socket accept() and recv() will block for a maximum of 1 second.  If you
    # omit this, it blocks indefinitely, waiting for a connection.
    sock.settimeout(1)

    while not signals["shutdown"]:
        print("listening")

        # Listen for a connection for 1s.  The socket library avoids consuming
        # CPU while waiting for a connection.
        try:
            clientsocket, address = sock.accept()
        except socket.timeout:
            continue
        print("Connection from", address[0])

        # Receive data, one chunk at a time.  If recv() times out before we can
        # read a chunk, then go back to the top of the loop and try again.
        # When the client closes the connection, recv() returns empty data,
        # which breaks out of the loop.  We make a simplifying assumption that
        # the client will always cleanly close the connection.
        message_chunks = []
        while True:
            try:
                data = clientsocket.recv(4096)
            except socket.timeout:
                continue
            if not data:
                break
            message_chunks.append(data)
        clientsocket.close()

        # Decode list-of-byte-strings to UTF8 and parse JSON data
        message_bytes = b''.join(message_chunks)
        message_str = message_bytes.decode("utf-8")
        message_dict = json.loads(message_str)
        print(message_dict)

    print("listen() shutting down")


if __name__ == "__main__":
    main()