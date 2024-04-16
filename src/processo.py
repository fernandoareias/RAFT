import sys
from libs.raft import Raft
import Pyro4
from libs.log import LogManager

if __name__ == '__main__': 
    node_id = sys.argv[2]
    logs = LogManager(node_id)
    raft = Raft(node_id, logs) # Nó
    
    print(f"[+][PROCESSO {raft.node_id}][{raft.state.name}] - Iniciando processo")


    daemon = Pyro4.Daemon()
    ns = Pyro4.locateNS()
    

    print(f"[+][PROCESSO {raft.node_id}][{raft.state.name}] - Registrando no daemon")
    uri = daemon.register(raft)

    print(f"[+][PROCESSO {raft.node_id}][{raft.state.name}] - Registrando no servidor de DNS")
    ns.register(raft.node_name, uri)

    raft.start(uri)
    daemon.requestLoop()