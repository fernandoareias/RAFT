import threading
import random
import time
import Pyro4 
from enum import Enum 

class RaftLogStatus(Enum):
    NOCOMMITTED = 1
    COMMITTED = 2

class RaftStatus(Enum):
        FOLLOWER = 1
        CANDIDATE = 2
        LEADER = 3
        
class Raft:
    PYRO_NS_URI = "PYRO:Pyro.NameServer@localhost:9090"
    node_id = 0
    node_name = ""
    node_uri = ""
    current_term = 0
    state = None
    voted_for = 0
    votes_received = 0
    leader_id = None
    leader_uri = ""
    last_leader_ping_time = None
    nodes = {}


    commands_queue = []
    lock = threading.Lock()
    
    __logManager = None

    #Inicializacao
    def __init__(self, node_id, logManager) -> None:
        self.node_id = node_id
        self.node_name = f"node_raft_{node_id}" 
        self.state = RaftStatus.FOLLOWER
        self.__logManager = logManager
       

    def start(self, node_uri):
         self.start_election_timer()
         self.node_uri = node_uri

    def has_elapsed_leader_ping_time(self, seconds) -> bool:
        if self.last_leader_ping_time is None:
            # Se nunca foi pingado pelo líder, então é seguro votar
            return True
        current_time = time.time()
        elapsed_time = current_time - self.last_leader_ping_time
        return elapsed_time >= seconds
    

    # Metodo para o cliente enviar o comando
    @Pyro4.expose
    def send_command(self, command) -> None:
        if(self.state != RaftStatus.LEADER):
            self.send_command_to_concensus_module(command)
            return
        else:
            self.concensus_module_receive_command(command)


    def send_command_to_concensus_module(self, command):

        if not self.leader_uri:
            print(f"Líder não encontrado no servidor de nomes, node_raft_{self.leader_id} , obj {self.leader_uri} ")
           

        # Conecta-se ao nó líder e envia o comando
        node_proxy = Pyro4.Proxy(self.leader_uri)
        print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Enviando comando '{command}' para o módulo de consenso no líder")
        node_proxy.concensus_module_receive_command(command)

    @Pyro4.expose
    def concensus_module_receive_command(self, command):
        print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Solicitacao de execucao de command recebida")

        if(self.state != RaftStatus.LEADER):
            return
        
        command_content = f"{self.current_term},{self.node_id},{command},{RaftLogStatus.NOCOMMITTED}"

        current_index = self.__logManager.write(command_content)

        # fase 1, escrita no arquivo de log
        self.send_write_command_log_to_followers(command_content, current_index)

        # fase 2, commit no arquivo de log
        self.send_commit_command_log_to_followers(current_index)

        self.__logManager.committed(current_index)

    def send_write_command_log_to_followers(self, command_content, log_index):
        self.nodes = self.search_nodes()

        print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Solicitando a escrita do log do command '{command_content}' de index {log_index}")

        write_erros = 0
        for node_name, node in self.nodes.items(): 
            # Envia solicitação de voto para cada nó
            try:
                node_proxy = Pyro4.Proxy(node)
                response = node_proxy.request_write_command_log(log_index, command_content)
                
                if response[0] == "invalid_index":
                    self.resend_command_to_followers(log_index)

                if response[0] == "write_error":
                    write_erros += 1

            except Exception as e:
                print(f"Erro na solicitacao de escrita do comando, ex {e}")
                write_erros += 1

        if write_erros > len(self.nodes) // 2:
            print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Falha na escrita de log do command '{command_content}' de index {log_index}")
            raise Exception("Mais de 50% dos nós encontraram erros ao escrever a alteração")
        
        

    def send_commit_command_log_to_followers(self, index):
        self.nodes = self.search_nodes()
        if(len(self.nodes) == 0 and self.state == RaftStatus.LEADER):
            response = self.request_commit_command_log(index)
            if response == "commit_error":
                raise Exception("Nao foi possivel commitar o command")
            return 
        
        print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Solicitando commit do log de index {index}")
        write_erros = 0
        for node_name, node in self.nodes.items(): 
            # Envia solicitação de voto para cada nó
            try:
                node_proxy = Pyro4.Proxy(node)
                response = node_proxy.request_commit_command_log(index)  
                if response[0] == "write_error":
                    write_erros += 1

            except Exception as e:
                write_erros += 1

        if write_erros > len(self.nodes) // 2:
            print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Erro no commit, mais de 50% dos nós encontraram erro ao comittar")
            self.state = RaftStatus.FOLLOWER
            raise Exception("Mais de 50% dos nós encontraram erros ao commitar a alteração")

    
    def resend_command_to_followers(self, last_index):
        if(self.state != RaftStatus.LEADER):
            return
        
        command = self.__logManager.read_command(last_index)

        self.send_write_command_log_to_followers(command)

        self.send_commit_command_log_to_followers(last_index)

        self.__logManager.committed(last_index)




    @Pyro4.expose
    def request_write_command_log(self, current_index, command):
        
        if self.__logManager.log_index < current_index - 1:
            return ("invalid_index", self.__logManager.log_index)
        try:
            self.__logManager.write(command)
            return ("written", self.__logManager.log_index)
        except:
            return ("write_error", self.__logManager.log_index)
        

    @Pyro4.expose
    def request_commit_command_log(self, log_index):
        try:
            self.__logManager.committed(log_index)
            print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Realizou o commit do log de index {log_index}")
            return "commited"
        except Exception as e:
            print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Falha no commit do log de index {log_index}, ex {e}")
            return "commit_error"
    
        


    @Pyro4.expose
    def request_vote(self, term, candidate_id):
        response = {"vote_granted": False}

        if not self.has_elapsed_leader_ping_time(15):
            return response

        if(self.state == RaftStatus.LEADER):
            return response 
        
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

        timeout = random.randint(50, 100) / 100
        self.heartbeat_timer = threading.Timer(timeout, self.send_heartbeat_broadcast)
        self.heartbeat_timer.start()

    def send_heartbeat_broadcast(self):
        try:
            self.heartbeat_timer.cancel()

            if(self.state != RaftStatus.LEADER):
                return
            
            self.nodes = self.search_nodes()

            if not self.nodes:
                return
            
            threads = []
            # Cria uma thread para cada nó na lista de nós
            for node_name, node_uri in self.nodes.items():
                thread = threading.Thread(target=self.send_heartbeat_to_node, args=(node_name, node_uri))
                threads.append(thread)
                thread.start()

            # Aguarda todas as threads terminarem
            for thread in threads:
                thread.join()

            #print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Heartbeat broadcast concluído.")
        except Exception as e:
            print(f"[-][PROCESSO {self.node_id}][{self.state.name}] - Erro no broadcast de heartbeat: {e}")
        finally:
            self.start_heartbeat_timer()

    def send_heartbeat_to_node(self, node_name, node_uri):
        try:
            # Conecta-se ao nó remoto usando Pyro4.Proxy
            node_proxy = Pyro4.Proxy(node_uri)
            # Envia a mensagem de heartbeat para o nó remoto
            node_proxy.receive_heartbeat(self.node_id, self.node_uri, self.current_term)
        except Exception as e:
            nameserver = Pyro4.locateNS()
            nameserver.remove(node_name)

    @Pyro4.expose
    def receive_heartbeat(self, node_leader_id, node_leader_uri, leader_term):
        #print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Heart beat recebido, lider {node_leader}")

        if self.state == RaftStatus.LEADER and node_leader_id != self.node_id and leader_term >= self.current_term:
            print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Rebaixando pois o lider {node_leader} possui um termo maior...")
            self.state = RaftStatus.FOLLOWER

        self.leader_uri = node_leader_uri
        self.leader_id = node_leader_id
        self.current_term = leader_term
        self.last_leader_ping_time = time.time()
        self.election_timer.cancel()
        self.start_election_timer()

    def start_election_timer(self) -> None:
        if(self.state == RaftStatus.CANDIDATE):
            self.state = RaftStatus.FOLLOWER

        timeout = random.randint(150, 300) / 100
        #print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Iniciando a proxima eleicao em {timeout}ms ...")
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
        if self.votes_received > len(self.nodes) / 2 :
            print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Votos suficientes recebidos. Tornando-se líder...")
            self.state = RaftStatus.LEADER
            self.leader_id = self.node_id
            print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Eleito como lider")
            self.start_heartbeat_timer()
        else:
            print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Votos insuficientes recebidos. Continuando como candidato...")
           

    # Envia a solicitacao de voto para o outro nó
    def send_request_vote(self):
        self.nodes = self.search_nodes()
        print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Quantidade de nos encontrados {len(self.nodes)}")

        for node_name, node in self.nodes.items(): 
            # Envia solicitação de voto para cada nó
            try:
                node_proxy = Pyro4.Proxy(node)
                response = node_proxy.request_vote(self.current_term, self.node_id)
                if response["vote_granted"]:
                    self.votes_received += 1  # Incrementa votos recebidos
            except Exception as e:
                ...

    def search_nodes(self) -> dict:
        node_dict = {}
        try:
            nameserver = Pyro4.Proxy(self.PYRO_NS_URI)
            objects = nameserver.list()

            for object_uri, object_name in objects.items():
                uri = str(object_uri)
                if(self.node_name != uri and uri.startswith("node_raft")):
                    node_dict[uri] = object_name
        except Exception as e:
            print(f"[+][PROCESSO {self.node_id}][{self.state.name}] - Erro na tentiva de listar os DNS no Pyro: {e}")
        finally:
            return node_dict
