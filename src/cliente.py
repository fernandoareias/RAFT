import Pyro4
import random
import sys
import datetime

def search_nodes() -> dict:
    node_dict = {}
    try:
        nameserver = Pyro4.Proxy("PYRO:Pyro.NameServer@localhost:9090")
        objects = nameserver.list()

        for object_uri, object_name in objects.items():
            uri = str(object_uri)
            if(uri.startswith("node_raft")):
                node_dict[uri] = object_name
    except Exception as e:
        print(f"[-] Erro na tentiva de listar os DNS no Pyro: {e}")
    finally:
        return node_dict


if __name__ == '__main__': 
    print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][CLIENTE] - Iniciando cliente...")
    x = 1
    while x <= 5:
        try:
            nodes = search_nodes()

            print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][CLIENTE] - Encontrou {len(nodes)} nós")
            uris = list(nodes.values())

            random_uri = random.choice(uris)

            print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][CLIENTE] - Vai se conectar com o nó {random_uri}")
            comando = sys.argv[1]
            node_proxy = Pyro4.Proxy(random_uri)
            node_proxy.send_command(f"COMANDO: {comando} {x}")
            print(f"[+][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][CLIENTE] - Comando processado com sucesso")
        except Exception as e:
            print(f"[-][{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][CLIENTE] - Falha no envio do comando, cluster nao possui lider definido no momento, tente novamente em alguns minutos")
        x += 1