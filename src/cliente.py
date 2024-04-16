

import sys
import Pyro4
import random

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
    print("[+] Iniciando cliente...")

    try:
        nodes = search_nodes()

        print(f"Encontrou {len(nodes)} nós")
        uris = list(nodes.values())

        random_uri = random.choice(uris)

        print(f"Vai se conectar com o nó {random_uri}")

        node_proxy = Pyro4.Proxy(random_uri)
        response = node_proxy.send_command("COMANDO: EXECUTAR XPTO")
        print(response)
    except Exception as e:
        print("Erro ao enviar comando:", e)