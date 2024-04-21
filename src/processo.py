import sys
from libs.raft import Raft
import Pyro4
from libs.log import LogManager
import datetime 

if __name__ == '__main__': 
    node_id = sys.argv[2]
    logs = LogManager(node_id)
    raft = Raft(node_id, logs) 
    
    print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {raft.node_id}][{raft.state.name}] - Iniciando processo")


    daemon = Pyro4.Daemon()
    ns = Pyro4.locateNS()
    

    print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {raft.node_id}][{raft.state.name}] - Registrando no daemon")
    uri = daemon.register(raft)
    raft.start(uri)

    print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {raft.node_id}][{raft.state.name}] - Registrando no servidor de DNS com nome {raft.node_name} e uri {raft.node_uri}")
    ns.register(raft.node_name, raft.node_uri)

    daemon.requestLoop()