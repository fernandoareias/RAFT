import os
from libs.raft import RaftLogStatus
import datetime
class LogManager:
    nome_arquivo = ""
    log_index = 0


    def __init__(self, node_id) -> None:
        self.nome_arquivo = f"src/logs/logs_node_raft_{node_id}.txt"
        if os.path.exists(self.nome_arquivo):
            print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][Log Manager] - O arquivo {self.nome_arquivo} já existe.")
            # Calcula o número da última linha do arquivo
            with open(self.nome_arquivo, 'r') as arquivo:
                for line in arquivo:
                    self.log_index += 1 
            print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][Log Manager] - O número da última linha do arquivo é: {self.log_index}")
        else:
            with open(self.nome_arquivo, 'w') as arquivo:
                print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][Log Manager] - O arquivo {self.nome_arquivo} foi criado.")
        
        if self.log_index == 0:
            self.log_index = 1

    def write(self, entry, index) -> int:
        if index < self.log_index:
            print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][Log Manager] - Ignorou a escrita do comando {entry}, index atual {self.log_index}, index informado {index}")
            return self.log_index

        # Verifica se já existe um log commitado no índice especificado
        if self.check_if_log_committed(index):
            print(f"[+][Log Manager] - Ignorou a escrita do comando {entry}, pois já existe um log commitado no índice {index}")
            return self.log_index

        try:
            with open(self.nome_arquivo, 'a') as arquivo:
                print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][Log Manager] - Index atual do arquivo de log e {self.log_index}")
                arquivo.write(entry + '\n')
                print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][Log Manager] - Realizou a escrita do log {entry} de index {self.log_index}")
        except Exception as e: 
            print(f"[-][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][Log Manager] - Exception na escrita do log {entry}, index informado {index} atual index do arquivo {self.log_index}, ex {e}")
        finally:
            return self.log_index

    def check_if_log_committed(self, index):
        if index <= 0:
            return False
        try:
            with open(self.nome_arquivo, 'r') as arquivo:
                linhas = arquivo.readlines()
                if len(linhas) >= index:
                    last_log = linhas[index - 1]
                    return last_log.endswith(str(RaftLogStatus.COMMITTED))
                else:
                    return False
        except Exception as e:
            print(f"[-][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][Log Manager] - Exception ao verificar log commitado, index informado {index}: {e}")
            return False

    
    def cancel_commit(self, log_index):
        if log_index != self.log_index:
            print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][Log Manager] - Ignorou o cancelamento de commit, index atual {self.log_index}, index informado {log_index}")
            return
            
        try:
            # Lê todas as linhas do arquivo
            with open(self.nome_arquivo, 'r') as arquivo:
                linhas = arquivo.readlines()

            # Apaga a linha desejada
            del linhas[log_index]

            # Escreve todas as linhas de volta para o arquivo
            with open(self.nome_arquivo, 'w') as arquivo:
                arquivo.writelines(linhas)

            self.log_index -= 1
            print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][Log Manager] - Realizou o cancelamento de commit do log de index {log_index}")
        except Exception as e:  
            print(f"[-][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][Log Manager] - Exception na cancelamento do commit, index informado {log_index}, atual index do arquivo {self.log_index}, ex {e}")


    def committed(self, line_number):

        if line_number != self.log_index:
            print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][Log Manager] - Ignorou o commit, index atual {self.log_index}, index informado {line_number}")
            return
        
        try:
            # Lê todas as linhas do arquivo
            with open(self.nome_arquivo, 'r') as arquivo:
                linhas = arquivo.readlines()

            # Modifica a linha desejada
            linhas[line_number - 1] = linhas[line_number - 1].replace(str(RaftLogStatus.NOCOMMITTED), str(RaftLogStatus.COMMITTED))

            # Escreve todas as linhas de volta para o arquivo
            with open(self.nome_arquivo, 'w') as arquivo:
                arquivo.writelines(linhas)

            print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][Log Manager] - Realizou o commit do log de index {line_number}")
            self.log_index += 1
        except Exception as e: 
             print(f"[-][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][Log Manager] - Exception no commit do log, index informado {line_number} atual index do arquivo {self.log_index}, ex {e}")
       


    
    def read_command(self, line_number):
        print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][Log Manager] - Buscando o comando no arquivo de log na linha {line_number}")
        with open(self.nome_arquivo, 'r') as arquivo:
            # Percorre as linhas até a linha desejada
            for _ in range(line_number):
                arquivo.readline()
            
            # Lê a linha desejada
            linha = arquivo.readline().strip()
            
            # Extrai e retorna o comando da linha no formato "{termo},{node_id},{command},{status}"
            _, _, command, _ = linha.split(',', 3)
            return command
 