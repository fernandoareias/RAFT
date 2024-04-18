import threading
import random
import time
import Pyro4 
from enum import Enum 
import datetime

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
           
        node_proxy = Pyro4.Proxy(self.leader_uri)
        print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}] - Enviando comando '{command}' para o módulo de consenso no líder")
        node_proxy.concensus_module_receive_command(command)

    @Pyro4.expose
    def concensus_module_receive_command(self, command):
        print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}] - Solicitacao de execucao de command recebida")

        if(self.state != RaftStatus.LEADER):
            return
        
        command_content = f"{self.current_term},{self.node_id},{command},{RaftLogStatus.NOCOMMITTED}"

        current_index = self.__logManager.write(command_content, self.__logManager.log_index)

        # fase 1, escrita no arquivo de log
        self.send_write_command_log_to_followers(command_content, current_index, "ENVIO")

        # fase 2, commit no arquivo de log
        self.send_commit_command_log_to_followers(current_index, "ENVIO")

        self.__logManager.committed(current_index)

    def send_write_command_log_to_followers(self, command_content, log_index, origem):

        if(len(self.nodes) == 0 and self.state == RaftStatus.LEADER):
            return 
        
        self.nodes = self.search_nodes()

        print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}][{origem}] - Solicitando a escrita do log do command '{command_content}' de index {log_index}")

        write_erros = 0
        invalid_index_erro = False
        for node_name, node in self.nodes.items(): 
            # Envia solicitação de voto para cada nó
            try:
                node_proxy = Pyro4.Proxy(node)
                response = node_proxy.request_write_command_log(log_index, command_content)
                
                if response[0] == "invalid_index":
                    invalid_index_erro = True
                    break 

                if response[0] == "write_error":
                    write_erros += 1

            except Exception as e:
                print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}][{origem}] - Erro na solicitacao de escrita do comando, ex {e}")
                write_erros += 1

        if(invalid_index_erro):
            self.send_cancel_commit(log_index)
            self.resend_command_to_followers(response[1], log_index)
            return

        if write_erros > len(self.nodes) // 2:
            print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}][{origem}] - Falha na escrita de log do command '{command_content}' de index {log_index}")
            self.send_cancel_commit(log_index)
            raise Exception("Mais de 50% dos nós encontraram erros ao escrever a alteração")
        
        

    def send_commit_command_log_to_followers(self, index, origem):
        self.nodes = self.search_nodes()

        if(len(self.nodes) == 0 and self.state == RaftStatus.LEADER):
            return 
        # if(len(self.nodes) == 0 and self.state == RaftStatus.LEADER):
        #     response = self.request_commit_command_log(index, origem)
        #     if response != "commited":
        #         raise Exception("Nao foi possivel commitar o command")
        #     return 
        
        print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}][{origem}] - Solicitando commit do log de index {index}")
        write_erros = 0
        for node_name, node in self.nodes.items(): 
            # Envia solicitação de voto para cada nó
            try:
                node_proxy = Pyro4.Proxy(node)
                response = node_proxy.request_commit_command_log(index, origem)  
                if response != "commited":
                    write_erros += 1

            except Exception as e:
                write_erros += 1
                print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}][{origem}] - Erro no commit, ex {e}")


        if write_erros > len(self.nodes) // 2:
            print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}][{origem}] - Erro no commit, mais de 50% dos nós encontraram erro ao comittar")
            self.state = RaftStatus.FOLLOWER
            raise Exception("Mais de 50% dos nós encontraram erros ao commitar a alteração")

    
    def resend_command_to_followers(self, start_index, last_index):

        print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}][REENVIO] - Reenviando todos os comandos a partir do index {start_index}")
        if(self.state != RaftStatus.LEADER):
            return
        current_index = start_index 
        while current_index < last_index:
            command = self.__logManager.read_command(current_index)
            command_content = f"{self.current_term},{self.node_id},{command},{RaftLogStatus.NOCOMMITTED}"
            try:
                print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}][REENVIO] - Encontrou o comando '{command}' na posicao {current_index} para reenvio")
                
                self.send_write_command_log_to_followers(command_content, current_index, "REENVIO")
                self.send_commit_command_log_to_followers(current_index, "REENVIO")

                current_index += 1
            except Exception as e:
                #current_index -= 1
                print(f"[-][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}][REENVIO] - Erro no reenvio dos comando | log index {current_index}, comando {command} |  ex {e}")

        
    def send_cancel_commit(self, log_index):
        self.nodes = self.search_nodes()

        if(len(self.nodes) == 0 and self.state == RaftStatus.LEADER):
            response = self.request_cancel_commit(log_index)
            if response != "commited_cancel":
                raise Exception(f"Nao foi possivel cancelar o commit do command no index {log_index}")
            return 
        
        print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}] - Solicitando cancelamento do commit do log de index {log_index}")
        cancel_commit_erros = 0
        for node_name, node in self.nodes.items(): 
            try:
                node_proxy = Pyro4.Proxy(node)
                response = node_proxy.request_cancel_commit(log_index)  
                if response != "commited_cancel":
                    #write_erros += 1
                    cancel_commit_erros += 1

            except Exception as e:
                cancel_commit_erros += 1
                print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}] - Erro no commit, ex {e}")
        
        if cancel_commit_erros > 0:
            raise Exception(f"Nao foi possivel cancelar o commit do command no index {log_index}, quantidade de erros {cancel_commit_erros}")

    @Pyro4.expose
    def request_cancel_commit(self, log_index):
        try:
            print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}] - Recebeu solicitacao de commit do log de index {log_index}")
            self.__logManager.cancel_commit(log_index)
            return "commited_cancel"
        except Exception as e:
            print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}] - Falha no cancelamento do commit do log de index {log_index}, ex {e}")
            return "cancel_commit_error"

    @Pyro4.expose
    def request_write_command_log(self, current_index, command):
        #print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}] - Recebeu solicitacao de escrita do log de index {current_index}, index atual LOG INDEX - {self.__logManager.log_index}, comando {command}")
        if self.__logManager.log_index < current_index - 1:
            return ("invalid_index", self.__logManager.log_index)
        try:
            self.__logManager.write(command, current_index)
            return ("written", self.__logManager.log_index)
        except:
            return ("write_error", self.__logManager.log_index)
        

    @Pyro4.expose
    def request_commit_command_log(self, log_index, origem):
        try:
            print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}][{origem}] - Solicitacao de commit de command recebida, index {log_index}")
            self.__logManager.committed(log_index)
            return "commited"
        except Exception as e:
            print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}][{origem}]  - Falha no commit do log de index {log_index}, ex {e}")
            return "commit_error"
    
        


    @Pyro4.expose
    def request_vote(self, term, candidate_id, candidate_log_index):
        response = {"vote_granted": False}

        if not self.has_elapsed_leader_ping_time(15):
            return response

        if(self.state == RaftStatus.LEADER):
            return response 
        
        if term > self.current_term:
            self.current_term = term
            self.state = RaftStatus.FOLLOWER
            self.voted_for = None  # Resetar voto para este termo
        if (self.voted_for is None or self.voted_for == candidate_id) and term == self.current_term and candidate_log_index >= self.__logManager.log_index:
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

            #print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}] - Encontrou {len(self.nodes.items())} nós para heart beat.")
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

            #print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}] - Heartbeat broadcast concluído.")
        except Exception as e:
            print(f"[-][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}] - Erro no broadcast de heartbeat: {e}")
        finally:
            self.start_heartbeat_timer()


    def send_heartbeat_to_node(self, node_name, node_uri):
        try:
            # Conecta-se ao nó remoto usando Pyro4.Proxy
            node_proxy = Pyro4.Proxy(node_uri)
            # Envia a mensagem de heartbeat para o nó remoto
            response = node_proxy.receive_heartbeat(self.node_id, self.node_uri, self.current_term, self.__logManager.log_index)

            if(response == "invalid_leader"):
                print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}] - Rebaixando pois o lider {self.node_id} pois o nó {node_name} possui um log mais atualizado...")
                self.state = RaftStatus.FOLLOWER

        except Exception as e:
            nameserver = Pyro4.locateNS()
            nameserver.remove(node_name)

    @Pyro4.expose
    def receive_heartbeat(self, node_leader_id, node_leader_uri, leader_term, leader_log_index) -> str:
        #print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}] - Heart beat recebido, lider {node_leader_id}, termo do lider {leader_term}, log index do lider {leader_log_index}")

        if self.state == RaftStatus.LEADER and node_leader_id != self.node_id and self.current_term > leader_term:
            self.election_timer.cancel()
            self.start_election_timer()
            return "invalid_leader"

        if leader_log_index < self.__logManager.log_index:
            self.election_timer.cancel()
            self.start_election_timer()
            return "invalid_leader"
        
        if self.state == RaftStatus.LEADER and node_leader_id != self.node_id and leader_term < self.current_term:
            print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}] - Rebaixando pois o lider {node_leader_id} possui um termo maior...")
            self.state = RaftStatus.FOLLOWER

        
        
        self.leader_uri = node_leader_uri
        self.leader_id = node_leader_id
        self.current_term = leader_term
        self.last_leader_ping_time = time.time()
        self.election_timer.cancel()
        self.start_election_timer()

        return "success"

    def start_election_timer(self) -> None:
        if(self.state == RaftStatus.CANDIDATE):
            self.state = RaftStatus.FOLLOWER

        timeout = random.randint(150, 300) / 100
        #print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}] - Iniciando a proxima eleicao em {timeout}ms ...")
        self.election_timer = threading.Timer(timeout, self.start_election)
        self.election_timer.start()

    def start_election(self): 
        print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}] - Iniciando nova eleição...")
        self.election_timer.cancel()  
 
        self.state = RaftStatus.CANDIDATE
        self.current_term += 1

        print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}][TERMO {self.current_term}] - Termo atual {self.current_term}")

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
            print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}] - Votos suficientes recebidos. Tornando-se líder...")
            self.state = RaftStatus.LEADER
            self.leader_id = self.node_id
            print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}] - Eleito como lider")
            self.start_heartbeat_timer()
        else:
            print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}] - Votos insuficientes recebidos. Continuando como candidato...")
        

    # Envia a solicitacao de voto para o outro nó
    def send_request_vote(self):
        self.nodes = self.search_nodes()
        print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}] - Quantidade de nos encontrados {len(self.nodes)}")

        for node_name, node in self.nodes.items(): 
            # Envia solicitação de voto para cada nó
            try:
                node_proxy = Pyro4.Proxy(node)
                response = node_proxy.request_vote(self.current_term, self.node_id, self.__logManager.log_index)
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
            print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][PROCESSO {self.node_id}][{self.state.name}][TERMO {self.current_term}][LOG INDEX - {self.__logManager.log_index}] - Erro na tentiva de listar os DNS no Pyro: {e}")
            ...
        finally:
            return node_dict
