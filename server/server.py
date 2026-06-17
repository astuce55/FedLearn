"""
server.py — Serveur central. Version réseau réel.
Ecoute sur 0.0.0.0 pour accepter les connexions de n'importe quelle machine du réseau.
"""
import grpc
import uuid
import logging
import socket
from concurrent import futures
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import federated_pb2
from common import federated_pb2_grpc

logging.basicConfig(level=logging.INFO, format="[SERVEUR %(asctime)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


class FederatedServer(federated_pb2_grpc.FederatedLearningServiceServicer):

    def __init__(self):
        self.registry = {}
        log.info("Serveur pret. En attente de clients...")

    def RegisterClient(self, request, context):
        client_id = str(uuid.uuid4())[:8]
        region = request.attributes.get("region", "default")
        structured_name = f"/fl/{region}/client-{client_id}"

        # Récupérer l'IP réelle du client depuis le contexte gRPC
        # (ce que context.peer() retourne : "ipv4:192.168.1.x:PORT")
        peer = context.peer()  # ex: "ipv4:192.168.1.5:54321"
        ip_client = peer.split(":")[1] if ":" in peer else request.ip_address

        cpu = request.attributes.get("cpu_threads", "?")
        ram = request.attributes.get("ram_fraction", "?")

        self.registry[client_id] = {
            "name": request.client_name,
            "ip_reel": ip_client,
            "ip_declare": request.ip_address,
            "dataset_size": request.dataset_size,
            "attributes": dict(request.attributes),
            "structured_name": structured_name,
        }

        log.info(
            "NOUVEAU CLIENT !\n"
            "  Nom         : %s\n"
            "  IP reelle   : %s\n"
            "  ID attribue : %s\n"
            "  Chemin      : %s\n"
            "  Dataset     : %d exemples\n"
            "  Ressources  : %s threads CPU, %.0f%% RAM\n"
            "  Total reseau: %d clients connectes",
            request.client_name, ip_client, client_id, structured_name,
            request.dataset_size, cpu,
            float(ram) * 100 if ram != "?" else 0,
            len(self.registry)
        )

        return federated_pb2.RegisterResponse(
            success=True,
            assigned_id=client_id,
            structured_name=structured_name,
            lamport_timestamp=0,
        )

    def GetGlobalModel(self, request, context):
        return federated_pb2.ModelResponse(weights=[0.0]*10, round_number=0, lamport_timestamp=0)

    def SendLocalUpdate(self, request, context):
        nom = self.registry.get(request.client_id, {}).get("name", "?")
        log.info("Mise a jour recue de %s (round %d)", nom, request.round_number)
        return federated_pb2.UpdateAck(received=True, lamport_timestamp=0)

    def Heartbeat(self, request, context):
        return federated_pb2.HeartbeatResponse(alive=True, lamport_timestamp=0)

    def ElectionMessage(self, request, context):
        return federated_pb2.ElectionResponse(accepted=True, lamport_timestamp=0)


def demarrer_serveur(port=50051):
    # Afficher l'IP locale pour la communiquer aux camarades
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip_locale = s.getsockname()[0]
        s.close()
    except Exception:
        ip_locale = "127.0.0.1"

    serveur = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    federated_pb2_grpc.add_FederatedLearningServiceServicer_to_server(FederatedServer(), serveur)

    # 0.0.0.0 = écouter sur TOUTES les interfaces réseau (WiFi, Ethernet, etc.)
    # Indispensable pour que les camarades se connectent depuis leurs machines
    serveur.add_insecure_port(f"0.0.0.0:{port}")
    serveur.start()

    print("\n" + "="*55)
    print("  Serveur d'apprentissage federe demarre !")
    print("="*55)
    print(f"  Communique cette adresse a tes camarades :")
    print(f"  >>> {ip_locale}:{port} <<<")
    print("="*55)
    print("  En attente de connexions...\n")

    serveur.wait_for_termination()


if __name__ == "__main__":
    demarrer_serveur()