"""
server.py — Étape 3 : avec horloges de Lamport.
"""
import grpc, socket, logging
from concurrent import futures
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import federated_pb2, federated_pb2_grpc
from common.naming import AnnuaireUnifie
from common.lamport_clock import HorlogeLamport, JournalEvenements

logging.basicConfig(level=logging.INFO, format="[SERVEUR %(asctime)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


class FederatedServer(federated_pb2_grpc.FederatedLearningServiceServicer):

    def __init__(self):
        self.annuaire = AnnuaireUnifie()
        self.horloge  = HorlogeLamport("serveur")   # ← horloge du serveur
        self.journal  = JournalEvenements()
        log.info("Serveur initialisé. LC=%d", self.horloge.valeur)

    def RegisterClient(self, request, context):
        # Règle 3 : réception du message du client
        lc = self.horloge.apres_reception(request.lamport_timestamp)
        self.journal.enregistrer(lc, "serveur", "RegisterClient reçu", request.client_name)

        peer = context.peer()
        ip_client = peer.split(":")[1] if "ipv4:" in peer else request.ip_address
        attrs_sup = {k: v for k, v in request.attributes.items() if k != "region"}

        resultat = self.annuaire.enregistrer_client(
            nom=request.client_name, ip=ip_client, port=request.port,
            region=request.attributes.get("region", "default"),
            dataset_size=request.dataset_size,
            attrs_supplementaires=attrs_sup,
        )

        # Règle 2 : envoi de la réponse
        lc_reponse = self.horloge.avant_envoi()
        self.journal.enregistrer(lc_reponse, "serveur", "RegisterResponse envoyé", resultat["client_id"])

        log.info("Client enregistré | %s | id=%s | LC=%d",
                 request.client_name, resultat["client_id"], lc_reponse)

        return federated_pb2.RegisterResponse(
            success=True,
            assigned_id=resultat["client_id"],
            structured_name=resultat["chemin"],
            lamport_timestamp=lc_reponse,      # ← le client synchronisera son horloge
        )

    def GetGlobalModel(self, request, context):
        # Règle 3 : réception de la demande
        lc = self.horloge.apres_reception(request.lamport_timestamp)
        self.journal.enregistrer(lc, "serveur", "GetGlobalModel reçu", request.client_id)

        # Règle 2 : envoi du modèle
        lc_envoi = self.horloge.avant_envoi()
        self.journal.enregistrer(lc_envoi, "serveur", "Modèle global envoyé", f"LC={lc_envoi}")

        return federated_pb2.ModelResponse(
            weights=[0.0]*10, round_number=0,
            lamport_timestamp=lc_envoi,
        )

    def SendLocalUpdate(self, request, context):
        # Règle 3 : réception de la mise à jour
        lc = self.horloge.apres_reception(request.lamport_timestamp)
        info = self.annuaire.get_client(request.client_id)
        nom = info["name"] if info else "?"
        self.journal.enregistrer(lc, "serveur", "Update reçue", f"{nom} round={request.round_number}")

        log.info("Update reçue de %s | round=%d | LC=%d", nom, request.round_number, lc)

        lc_ack = self.horloge.avant_envoi()
        return federated_pb2.UpdateAck(received=True, lamport_timestamp=lc_ack)

    def Heartbeat(self, request, context):
        lc = self.horloge.apres_reception(request.lamport_timestamp)
        lc_rep = self.horloge.avant_envoi()
        return federated_pb2.HeartbeatResponse(alive=True, lamport_timestamp=lc_rep)

    def ElectionMessage(self, request, context):
        lc = self.horloge.apres_reception(request.lamport_timestamp)
        lc_rep = self.horloge.avant_envoi()
        return federated_pb2.ElectionResponse(accepted=True, lamport_timestamp=lc_rep)


def demarrer_serveur(port=50051):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80)); ip_locale = s.getsockname()[0]; s.close()
    except Exception:
        ip_locale = "127.0.0.1"

    serveur = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    federated_pb2_grpc.add_FederatedLearningServiceServicer_to_server(FederatedServer(), serveur)
    serveur.add_insecure_port(f"0.0.0.0:{port}")
    serveur.start()
    print(f"\n{'='*50}\n  Serveur démarré — {ip_locale}:{port}\n{'='*50}\n")
    serveur.wait_for_termination()


if __name__ == "__main__":
    demarrer_serveur()