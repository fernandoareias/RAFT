import threading
import random
import time
from ..libs.core import RaftStatus
import Pyro4 


class Raft:
    node_id = 0
    current_term = 0
    state = None
    voted_for = 0
    votes_received = 0

    def __init__(self, node_id) -> None:
        self.node_id = node_id

    def request_vote(self, candidate_id):
        # Lógica para processar uma solicitação de voto de outro nó
        pass

    def append_entries(self, leader_id):
        # Lógica para processar uma entrada de líder de outro nó
        pass

    def start_election_timer(self):
        timeout = random.randint(150, 300) / 1000  
        self.election_timer = threading.Timer(timeout, self.start_election)
        self.election_timer.start()

    def start_election(self): 
        print("[+] Iniciando nova eleição...")
        self.election_timer.cancel()  
 
       
        self.state = RaftStatus.CANDIDATE
        self.current_term += 1

        print(f"[+][PROCESSO {self.node_id}] Termo atual {self.current_term}")

        self.voted_for = self.node_id
        self.votes_received = 1  # Votar em si mesmo

        self.send_request_vote()

        # Reinicia o temporizador para aguardar respostas dos votos
        self.start_election_timer()


    def send_request_vote(self):
        nameserver = Pyro4.locateNS()
        objects = nameserver.list()

        raft_nodes = []
        for object_uri, object_name in objects.items():
            if object_name.startswith("node"):
                raft_nodes.append(object_name)

        # Imprima a lista de nós Raft
        print("Nós Raft registrados:")
        for node_id in raft_nodes:
            print(node_id)

