import socket
import json


"""Utils file.

This file is to house code common between the Master and the Worker

"""
def tcp_socket(port):
  """Initialize and bind a TCP socket for LISTENING."""
  # create a new tcp socket
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

  # bind socket to server
  sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
  sock.bind(("localhost", port))
  sock.listen()

  return sock


def listen_setup(sock):
    """Set up listening on a sock. Returns message as a string."""
    # listen for a connection
    try:
        clientsocket, address = sock.accept()
        print("Connection from", address[0])
    except socket.timeout:
        # print("Socket timeout")
        return ""
    

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

    return message_str

