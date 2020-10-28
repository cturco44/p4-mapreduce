import socket


"""Utils file.

This file is to house code common between the Master and the Worker

"""
def create_socket(port):
  """Initialize and bind a socket."""
  # create a new tcp socket
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

  # bind socket to server
  sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
  sock.bind(("localhost", port))
  sock.listen()

  return sock


def listen(signals, sock):
    """Wait on a message from a socket or a shutdown signal."""
    # as shown in example...
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
            data = clientsocket.recv(4096) # TODO: maximum size??
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
