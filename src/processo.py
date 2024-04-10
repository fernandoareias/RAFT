import sys
from libs.raft import Raft
import Pyro4

if __name__ == '__main__':
    global node_id 
    global status
    node_id = sys.argv[2]
    print(f"[+][PROCESSO {node_id}] - Iniciando processo {node_id}")

    raft = Raft(node_id) # Nó

    daemon = Pyro4.Daemon()

    print(f"[+][PROCESSO {node_id}] - Registrando no daemon {node_id}")
    daemon.register(raft)
    nameserver = Pyro4.locateNS()
    uri = nameserver.lookup("raft_node")

    print(f"[+][PROCESSO {node_id}] - Registrando no servidor de DNS {node_id}")
    

    nameserver.register(f"raft_node_{node_id}", uri)

    raft.start()
    daemon.requestLoop()