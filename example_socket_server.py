"""Example socket server."""
import socket
import json


def main():
    """Test Socket Server."""
    # Create an INET, STREAMing socket, this is TCP
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Bind the socket to the server
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("localhost", 8000))
    sock.listen()

    # Connect to a client
    clientsocket, address = sock.accept()
    print("Connection from", address[0])

    # Receive data, one chunk at a time.  When the client closes the
    # connection, recv() returns empty data, which breaks out of the loop.  We
    # make a simplifying assumption that the client will always cleanly close
    # the connection.
    message_chunks = []
    while True:
        data = clientsocket.recv(4096)
        if not data:
            break
        message_chunks.append(data)
    clientsocket.close()

    # Decode UTF8 and parse JSON data
    message_bytes = b''.join(message_chunks)
    message_str = message_bytes.decode("utf-8")
    message_dict = json.loads(message_str)
    print(message_dict)


if __name__ == "__main__":
    main()