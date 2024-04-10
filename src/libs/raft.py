import threading
import random
import time
from libs.core import RaftStatus
import Pyro4 


class Raft:
    node_id = 0
    current_term = 0
    state = None
    voted_for = 0
    votes_received = 0
    nodes = []

    def __init__(self, node_id,) -> None:
        self.node_id = node_id
        self.state = RaftStatus.FOLLOWER
       

    def start(self):
         self.start_election_timer()

    @Pyro4.expose
    def request_vote(self, term, candidate_id):
        """
        Método chamado por outros nós para votar em um candidato.
        """
        response = {"vote_granted": False}
        if term > self.current_term:
            self.current_term = term
            self.state = RaftStatus.FOLLOWER
            self.voted_for = None  # Resetar voto para este termo
        if (self.voted_for is None or self.voted_for == candidate_id) and term == self.current_term:
            # Conceder voto se não votou ainda neste termo e se candidato está no mesmo termo
            self.voted_for = candidate_id
            response["vote_granted"] = True
        return response
    
    def start_heartbeat_timer(self) -> None:
        if(self.state != RaftStatus.LEADER):
            return

        timeout = random.randint(50, 100) / 10
        self.heartbeat_timer = threading.Timer(timeout, self.send_heartbeat)
        self.heartbeat_timer.start()

    def send_heartbeat(self) -> None:
        try:
            self.heartbeat_timer.cancel()

            print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Enviando heart beat...")
            if(self.state != RaftStatus.LEADER):
                return

            self.nodes = self.search_nodes()

            if not self.nodes:
                return
        finally:
            self.start_heartbeat_timer()

    def append_entries(self, leader_id) -> None:
        # Lógica para processar uma entrada de líder de outro nó
        pass

    def start_election_timer(self) -> None:
        if(self.state == RaftStatus.CANDIDATE):
            self.state = RaftStatus.FOLLOWER

        timeout = random.randint(150, 300) / 10
        print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Iniciando a proxima eleicao em {timeout}ms ...")
        self.election_timer = threading.Timer(timeout, self.start_election)
        self.election_timer.start()

    def start_election(self): 
        print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Iniciando nova eleição...")
        self.election_timer.cancel()  
 
        self.state = RaftStatus.CANDIDATE
        self.current_term += 1

        print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Termo atual {self.current_term}")

        self.voted_for = self.node_id
        self.votes_received = 1  # Votar em si mesmo

        # Envia a solicitacao de votos para todos os nos
        self.send_request_vote()

        # Realiza a contagem dos votos e atualiza o status
        self.handle_vote_response()

        # Reinicia o temporizador para aguardar respostas dos votos
        if(self.state != RaftStatus.LEADER):
            self.start_election_timer()


    def handle_vote_response(self):
        if self.state != RaftStatus.CANDIDATE:
            return  # Este nó não é candidato

        # Verifica se recebeu votos suficientes para se tornar líder
        if self.votes_received > len(self.nodes) / 2:
            print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Votos suficientes recebidos. Tornando-se líder...")
            self.state = RaftStatus.LEADER
            self.start_heartbeat_timer()
        else:
            print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Votos insuficientes recebidos. Continuando como candidato...")
           

    # Envia a solicitacao de voto para o outro nó
    def send_request_vote(self):
        # Busca a lista atualizada de nos e adiciona em uma lista
        
        self.nodes = self.search_nodes()
 
        for node_id in self.nodes: 
            # Envia solicitação de voto para cada nó
            try:
                node_proxy = Pyro4.Proxy(objects[node_id])
                response = node_proxy.request_vote(self.current_term, self.node_id)
                if response["vote_granted"]:
                    self.votes_received += 1  # Incrementa votos recebidos
            except Exception as e:
                print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Erro ao enviar solicitação de voto para processo {node_id}: {e}")


    def search_nodes(self) -> list:
        node_list = []
        try:
            nameserver = Pyro4.locateNS()
            objects = nameserver.list()

            for object_uri, object_name in objects.items():
                if object_name.startswith("node"):
                    node_list.append(object_name)
            
            print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Quantidade de nos encontrados {len(node_list)}")
        except Exception as e:
            print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Erro na tentiva de listar os DNS no Pyro: {e}")
        finally:
            return node_list
