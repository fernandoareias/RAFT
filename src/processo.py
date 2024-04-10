import sys
from libs.raft import Raft
import Pyro4

if __name__ == '__main__':
    global node_id 
    global status
    node_id = sys.argv[2]
    print(f"[LOG] - Iniciando processo {node_id}")

    raft = Raft(node_id) # Nó

    daemon = Pyro4.Daemon()
    uri = daemon.register(raft)

    nameserver = Pyro4.locateNS()
    nameserver.register(node_id, uri)