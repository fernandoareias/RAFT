import os
from libs.raft import RaftLogStatus

class LogManager:
    nome_arquivo = ""
    log_index = 0


    def __init__(self, node_id) -> None:
        self.nome_arquivo = f"src/logs/logs_node_raft_{node_id}.txt"
        if os.path.exists(self.nome_arquivo):
            print(f'O arquivo {self.nome_arquivo} já existe.')
            # Calcula o número da última linha do arquivo
            with open(self.nome_arquivo, 'r') as arquivo:
                for line in arquivo:
                    self.log_index += 1
            print(f'O número da última linha do arquivo é: {self.log_index}')
        else:
            with open(self.nome_arquivo, 'w') as arquivo:
                print(f'O arquivo {self.nome_arquivo} foi criado.')

    def write(self, entry) -> int:
        with open(self.nome_arquivo, 'a') as arquivo:
            print(f"Index atual do arquivo de log e {self.log_index}")
            arquivo.write(entry + '\n')
            self.log_index += 1
            return self.log_index
        
    def committed(self, line_number):
        # Lê todas as linhas do arquivo
        with open(self.nome_arquivo, 'r') as arquivo:
            linhas = arquivo.readlines()

        print(f"RaftLogStatus.NOCOMMITTED.value {RaftLogStatus.NOCOMMITTED}")
        print(f"RaftLogStatus.COMMITTED.value {RaftLogStatus.COMMITTED}")
        # Modifica a linha desejada
        linhas[line_number - 1] = linhas[line_number - 1].replace(str(RaftLogStatus.NOCOMMITTED), str(RaftLogStatus.COMMITTED))

        # Escreve todas as linhas de volta para o arquivo
        with open(self.nome_arquivo, 'w') as arquivo:
            arquivo.writelines(linhas)


    
    def read_command(self, line_number):
        with open(self.__logManager.nome_arquivo, 'r') as arquivo:
            # Percorre as linhas até a linha desejada
            for _ in range(line_number - 1):
                arquivo.readline()
            
            # Lê a linha desejada
            linha = arquivo.readline().strip()
            
            # Extrai e retorna o comando da linha no formato "{termo},{node_id},{command},{status}"
            _, _, command, _ = linha.split(',', 3)
            return command


    def read_log(self):
        with open(self.nome_arquivo, 'r') as arquivo:
            return arquivo.readlines()

    def clear_log(self):
        with open(self.nome_arquivo, 'w') as arquivo:
            arquivo.write('')
