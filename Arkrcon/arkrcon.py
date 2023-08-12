"""
ARK Server setup on Gentoo:

Steps:
- Install Gentoo
- Setup steamcmd according to https://wiki.gentoo.org/wiki/Steamcmd
- Follow steps to set up and download ARK server files according to https://ark.wiki.gg/wiki/Dedicated_server_setup
    - Be sure to increase file limit via limits.conf
- Run ShooterGame under ./ShooterGame/Binaries/Linux via an sh script described at link from previous step
    - RCON Port is for remote console
    - Query port is for detecting the server in the server list
    - Regular Port is for actually connecting to the game
    - The last two need to be port forwarded for people to play
- Close the server via CTRL-C and configure the .ini files following https://ark.fandom.com/wiki/Server_configuration
- Saved arks are stores as .ark files under the Saved folder. AltSaveDirectoryName can be used as a cmd arg to change
the directory, but the default one is Saved/SavedArks
    - .arkprofile stores the character data associated with a steamid in the filename
    - You can specify the map as part of the server config. This will cause the server to look for the latest .ark save
    file for that map in the specified save directory. If not found, it will create one. It will also read ark profiles
    and ini configs. This means that you can change the map which will load a different map save but the arkprofile files
    will be read the same
    - Using two different Alt Save Directories will obviously look in different folders, so it wont find arkprofiles or anything for that matter (except config will still stay in common)
- To build a cluster, you add a clusterid cmd arg to the servers you are running. You should also have different alt save directories for the two servers to avoid any potential save clashing
    - The clusterid arguments generates a folder under Saved/clusters/<>
    - All servers in the cluster must share the same ID.
    - When you transfer a character, dino, or item, it gets saved to the cluster folder
    - Then on the other server, the content is downloaded
    - Clusters are known to sometimes have issues over LAN:
        - https://survivetheark.com/index.php?/forums/topic/87419-guide-cluster-setup/
        - https://survivetheark.com/index.php?/forums/topic/460704-solved-arkmanager-cluster-setup/
        - https://www.reddit.com/r/ARK/comments/rghbzi/cluster_servers_and_lans_is_there_any_news/
        - https://survivetheark.com/index.php?/forums/topic/197959-multiple-servers-over-lan/
    - However, port forwarding the servers AND connecting to it via steam:connect url via the public IP solved this issue for me.
"""

import socket
import sys
import os
from threading import RLock, Thread
from dataclasses import dataclass
import re
import time
from typing import Optional


class SyncPrinter:
    def __init__(self):
        self.__print_lock = RLock()

    def get_sync_input(self, prompt: str) -> str:
        """Gets user input while preventing anything from being printed in the meantime. Returns the input stripped"""
        with self.__print_lock:
            o = input(prompt).strip()
        return o

    def print_safe(self, msg):
        """Thread safe printing"""
        with self.__print_lock:
            print(msg)


class RCON:
    """Implements RCON protocol as outlined here https://developer.valvesoftware.com/wiki/Source_RCON_Protocol"""
    SERVERDATA_AUTH = 3
    SERVERDATA_AUTH_RESPONSE = 2
    SERVERDATA_EXECCOMMAND = 2
    SERVERDATA_RESPONSE_VALUE = 0

    @dataclass
    class PacketData:
        size: int
        pack_id: int
        pack_type: int
        body: str

    def __init__(self, rcon_ip: str, rcon_port: int, rcon_password: str, sync_printer: SyncPrinter):
        self.ip = rcon_ip
        self.port = rcon_port
        self.password = rcon_password

        self.__printer = sync_printer
        self.__sock: Optional[socket.socket] = None

        # Flag to determine if things are running
        self.__running_lock = RLock()
        self.running = True

        # IDs of all server replies. Lets you check if your request has been given a reply
        self.__responsed_to = []
        self.__responded_to_lock = RLock()

    def __set_responded(self, pack_id: int):
        with self.__responded_to_lock:
            self.__responsed_to.append(pack_id)

    def is_responded(self, pack_id: int) -> bool:
        """returns if the given packet ID has received a response from the server"""
        with self.__responded_to_lock:
            t = pack_id in self.__responsed_to
        return t

    def is_running(self):
        """Thread-safely return the value of the running flag (used to stop reception thread)"""
        with self.__running_lock:
            temp = self.running
        return temp

    def set_running(self, val: bool):
        """Can be used to stop the reception thread"""
        with self.__running_lock:
            self.running = val

    def __print_with_prefix(self, msg: str, pack_id: Optional[int] = None, pack_type: Optional[int] = None):
        """Prints message with a prefix"""
        add = "" + \
              (f", ID={pack_id}" if pack_id is not None else "") + \
              (f", TYP={pack_type}" if pack_type is not None else "")
        self.__printer.print_safe(f"[Server {self.ip}:{self.port}{add}] {msg}")

    def __reception(self, sock: socket.socket):
        """Repeatedly receives and parses server RCON packets"""
        state = 0  # 0 = reading size, 1 = reading id, 2 = reading type, 3 = reading body, 4 = reading null terminator
        cur_item = bytearray()  # Keeps track of the current item being read

        cur_struct = RCON.PacketData(None, None, None, None)

        while self.running:
            buf = sock.recv(4096)
            #self.print_safe(f"BUF: {buf}, CUR_ITEM: {cur_item}, STATE: {state}")
            if not buf:
                if self.disconnect():
                    self.__print_with_prefix("Connection closed on other end, stopping reception...")
                break

            for b in buf:
                if state == 0 and len(cur_item) < 4:  # Expecting integer (4 bytes)
                    cur_item.append(b)
                    if len(cur_item) == 4:
                        state = 1
                        cur_struct.size = int.from_bytes(cur_item, "little", signed=False)
                        cur_item = bytearray()
                elif state == 1 and len(cur_item) < 4:  # Expecting integer (4 bytes)
                    cur_item.append(b)
                    if len(cur_item) == 4:
                        state = 2
                        cur_struct.pack_id = int.from_bytes(cur_item, "little", signed=False)
                        cur_item = bytearray()
                elif state == 2 and len(cur_item) < 4:  # Expecting integer (4 bytes)
                    cur_item.append(b)
                    if len(cur_item) == 4:
                        state = 3
                        cur_struct.pack_type = int.from_bytes(cur_item, "little", signed=False)
                        cur_item = bytearray()
                elif state == 3 and len(cur_item) < cur_struct.size - 9:  # Expecting body (arbitrary size)
                    cur_item.append(b)
                    if len(cur_item) == cur_struct.size - 9:
                        #print("here", b)
                        state = 4
                        cur_struct.body = str(bytes(cur_item), "UTF-8")
                        cur_item = bytearray()
                elif state == 4:
                    self.__print_with_prefix(cur_struct.body, cur_struct.pack_id, cur_struct.pack_type)
                    self.__set_responded(cur_struct.pack_id)
                    cur_struct = RCON.PacketData(None, None, None, None)
                    state = 0

        self.__print_with_prefix("Reception thread closed.")

    def __build_packet(self, packet_id: int, packet_type: int, packet_body: str) -> bytes:
        """Follows the RCON protocol, look at link in class header"""
        body = bytes(packet_body, "UTF-8")
        size = len(body) + 10
        packet = int.to_bytes(size, 4, "little", signed=False) + int.to_bytes(packet_id, 4, "little", signed=False) + \
               int.to_bytes(packet_type, 4, "little", signed=False) + body + bytes([0]) + bytes([0])
        #self.print_safe(packet)
        return packet

    def send_cmd(self, pack_id: int, content: str):
        """Send an ARK console command specified by content, with the given pack_id"""
        self.__sock.send(self.__build_packet(pack_id, RCON.SERVERDATA_EXECCOMMAND, content))
        self.__print_with_prefix(f"Sent command '{content}'")

    def disconnect(self) -> bool:
        """Closes connection. Returns False if the connection had already been closed on this end"""
        try:
            self.__sock.shutdown(socket.SHUT_RDWR)
            self.__sock.close()
            self.__print_with_prefix("Disconnected Successfully.")
        except Exception as e:
            return False
        return True

    def connect(self):
        """Establishes a connection, sends password, launches reception thread"""
        # Connect the socket
        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__sock.connect((self.ip, self.port))
        self.__print_with_prefix(f"Connected to {self.ip}:{self.port} successfully!")

        # Start reception thread
        Thread(target=self.__reception, args=[self.__sock]).start()
        self.__print_with_prefix("Reception thread started.")

        # Send password
        pack_id = 50  # Arbitrary, but used to identify response
        self.__sock.send(self.__build_packet(pack_id, RCON.SERVERDATA_AUTH, self.password))
        self.__print_with_prefix("Password sent.")
        while not self.is_responded(50):
            time.sleep(0.2)


if __name__ == "__main__":
    # Check args
    if len(sys.argv) != 4:
        print("Not enough arguments! Correct usage:")
        print()
        print("> python3 arkrcon.py <server_ip> <rcon_port> <rcon_password>")
        exit(0)

    # Set args
    ip = sys.argv[1]
    port = int(sys.argv[2])
    password = sys.argv[3]

    printer = SyncPrinter()

    rcon = RCON(ip, port, password, printer)
    rcon.connect()

    printer.print_safe("Enter 'i' to input a command, 'q' to quit, or 's' to shutdown a cluster")

    pack_id = 69

    while True:
        dat = os.read(sys.stdin.fileno(), 10)
        cmd = str(dat, "UTF-8")[0]

        if cmd == 'q':
            printer.print_safe("Quit command issued.")
            rcon.set_running(False)
            break

        elif cmd == 'i':
            rcon_command = printer.get_sync_input("Enter an rcon command: ")
            rcon.send_cmd(pack_id, rcon_command)
            pack_id += 1

        elif cmd == 's':
            printer.print_safe("Cluster shutdown command initiated")
            printer.print_safe("All other servers must share the same IP and password, only port can vary.")
            shutdown_ports_raw = printer.get_sync_input(f"Enter a list of ports whitespace separated not including default port {port}, or 'c' to cancel: ")
            if shutdown_ports_raw == "c":
                printer.print_safe("Operation Cancelled.")
            else:
                # Parse ports
                shutdown_ports = re.split("[^0-9]+", shutdown_ports_raw)
                # For each port (set to remove duplicates), and includes default port
                for p in set(shutdown_ports + [port]):
                    # Connect via RCON. Ensures that a redundant connection isn't made to the same server
                    if int(p) == port:
                        conn = rcon
                    else:
                        conn = RCON(ip, int(p), password, printer)
                        conn.connect()
                    # Run saveworld, wait for reply, then run doexit and wait for reply
                    for cur_pack_id, cur_cmd in ((60, "saveworld"), (61, "doexit")):
                        conn.send_cmd(cur_pack_id, cur_cmd)
                        while not conn.is_responded(cur_pack_id):
                            print("Awaiting reply...")
                            time.sleep(2)
                    # Closes conn if not default
                    if int(p) != port:
                        conn.disconnect()
                printer.print_safe("Cluster shutdown finished.")
                # Breaks main loop since entire cluster is now closed
                break

    rcon.disconnect()
    printer.print_safe("Socket closed.")
