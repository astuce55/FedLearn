"""
server.py — Serveur central du système d'apprentissage fédéré.
"""
import grpc
import uuid
import logging
from concurrent import futures
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import federated_pb2
from common import federated_pb2_grpc

logging.basicConfig(level=logging.INFO, format="[SERVEUR %(asctime)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


class FederatedServer(federated_pb2_grpc.FederatedLearningServiceServicer):
    """
    Squelette (skeleton) du serveur.
    Hérite de FederatedLearningServiceServicer généré par grpc_tools.protoc.
    """

    def __init__(self):
        self.registry = {}   # client_id → infos
        log.info("Serveur initialisé. Registre vide.")

    def RegisterClient(self, request, context):
        # Nommage PLAT : UUID court
        client_id = str(uuid.uuid4())[:8]

        # Nommage STRUCTURÉ : chemin hiérarchique
        region = request.attributes.get("region", "default")
        structured_name = f"/fl/{region}/client-{client_id}"

        # Stockage dans le registre (nommage par ATTRIBUTS inclus)
        self.registry[client_id] = {
            "name": request.client_name,
            "ip": request.ip_address,
            "dataset_size": request.dataset_size,
            "attributes": dict(request.attributes),
            "structured_name": structured_name,
        }

        log.info("Client enregistré | id=%s | chemin=%s | dataset=%d | total=%d",
                 client_id, structured_name, request.dataset_size, len(self.registry))

        return federated_pb2.RegisterResponse(
            success=True,
            assigned_id=client_id,
            structured_name=structured_name,
            lamport_timestamp=0,
        )

    def GetGlobalModel(self, request, context):
        log.info("GetGlobalModel demandé par %s", request.client_id)
        return federated_pb2.ModelResponse(weights=[0.0]*10, round_number=0, lamport_timestamp=0)

    def SendLocalUpdate(self, request, context):
        log.info("Update reçue de %s (round %d)", request.client_id, request.round_number)
        return federated_pb2.UpdateAck(received=True, lamport_timestamp=0)

    def Heartbeat(self, request, context):
        return federated_pb2.HeartbeatResponse(alive=True, lamport_timestamp=0)

    def ElectionMessage(self, request, context):
        return federated_pb2.ElectionResponse(accepted=True, lamport_timestamp=0)


def demarrer_serveur(port=50051):
    serveur = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    federated_pb2_grpc.add_FederatedLearningServiceServicer_to_server(FederatedServer(), serveur)
    serveur.add_insecure_port(f"[::]:{port}")
    serveur.start()
    log.info("Serveur démarré sur le port %d. En attente de clients...", port)
    serveur.wait_for_termination()


if __name__ == "__main__":
    demarrer_serveur()