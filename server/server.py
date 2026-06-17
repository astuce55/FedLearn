"""
server.py — Étape 4 : avec FedAvg réel.
"""
import grpc, socket, logging, threading
from concurrent import futures
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import federated_pb2, federated_pb2_grpc
from common.naming import AnnuaireUnifie
from common.lamport_clock import HorlogeLamport, JournalEvenements
from server.aggregator import AggregateurFedAvg

logging.basicConfig(level=logging.INFO,
                    format="[SERVEUR %(asctime)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

NB_CLIENTS   = 3    # nombre de clients attendus par round
NB_ROUNDS    = 3    # nombre de rounds FL
TAILLE_MODEL = 2    # [w, b]


class FederatedServer(federated_pb2_grpc.FederatedLearningServiceServicer):

    def __init__(self):
        self.annuaire    = AnnuaireUnifie()
        self.horloge     = HorlogeLamport("serveur")
        self.journal     = JournalEvenements()
        self.aggregateur = AggregateurFedAvg(NB_CLIENTS, TAILLE_MODEL)
        self._round_lock = threading.Event()  # synchronisation des rounds
        self._round_actuel = 0
        self.aggregateur.demarrer_round()
        log.info("Serveur initialisé. Attente de %d clients.", NB_CLIENTS)

    # ── Register ──────────────────────────────────────────────
    def RegisterClient(self, request, context):
        lc = self.horloge.apres_reception(request.lamport_timestamp)
        self.journal.enregistrer(lc, "serveur", "RegisterClient", request.client_name)

        peer = context.peer()
        ip   = peer.split(":")[1] if "ipv4:" in peer else request.ip_address
        attrs = {k: v for k, v in request.attributes.items() if k != "region"}

        res = self.annuaire.enregistrer_client(
            nom=request.client_name, ip=ip, port=request.port,
            region=request.attributes.get("region", "default"),
            dataset_size=request.dataset_size,
            attrs_supplementaires=attrs,
        )
        lc_rep = self.horloge.avant_envoi()
        log.info("Client enregistré | %s | id=%s | LC=%d",
                 request.client_name, res["client_id"], lc_rep)

        return federated_pb2.RegisterResponse(
            success=True, assigned_id=res["client_id"],
            structured_name=res["chemin"], lamport_timestamp=lc_rep,
        )

    # ── GetGlobalModel ────────────────────────────────────────
    def GetGlobalModel(self, request, context):
        lc = self.horloge.apres_reception(request.lamport_timestamp)
        self.journal.enregistrer(lc, "serveur", "GetGlobalModel", request.client_id[:6])

        poids = list(self.aggregateur.poids_globaux)
        lc_rep = self.horloge.avant_envoi()

        log.info("Modèle envoyé | LC=%d | poids=%s",
                 lc_rep, [round(p, 4) for p in poids])
        return federated_pb2.ModelResponse(
            weights=poids,
            round_number=self._round_actuel,
            lamport_timestamp=lc_rep,
        )

    # ── SendLocalUpdate ───────────────────────────────────────
    def SendLocalUpdate(self, request, context):
        lc = self.horloge.apres_reception(request.lamport_timestamp)
        info = self.annuaire.get_client(request.client_id)
        nom  = info["name"] if info else "?"
        self.journal.enregistrer(lc, "serveur", "Update reçue",
                                 f"{nom} round={request.round_number}")

        # Ajouter à l'agrégateur
        complet = self.aggregateur.ajouter_update(
            client_id=request.client_id,
            poids=list(request.weights),
            n_k=request.dataset_size,
            lamport_ts=request.lamport_timestamp,
        )

        # Quand tous les clients ont envoyé → agréger
        if complet:
            nouveaux_poids = self.aggregateur.agreger()
            log.info("══ FedAvg round %d terminé → W=%s ══",
                     request.round_number,
                     [round(p, 4) for p in nouveaux_poids])
            # Préparer le round suivant
            self._round_actuel = request.round_number + 1
            self.aggregateur.demarrer_round()

        lc_ack = self.horloge.avant_envoi()
        return federated_pb2.UpdateAck(received=True, lamport_timestamp=lc_ack)

    # ── Heartbeat & Election ──────────────────────────────────
    def Heartbeat(self, request, context):
        lc = self.horloge.apres_reception(request.lamport_timestamp)
        return federated_pb2.HeartbeatResponse(
            alive=True, lamport_timestamp=self.horloge.avant_envoi())

    def ElectionMessage(self, request, context):
        lc = self.horloge.apres_reception(request.lamport_timestamp)
        return federated_pb2.ElectionResponse(
            accepted=True, lamport_timestamp=self.horloge.avant_envoi())


def demarrer_serveur(port=50051):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80)); ip = s.getsockname()[0]; s.close()
    except Exception:
        ip = "127.0.0.1"

    srv = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    federated_pb2_grpc.add_FederatedLearningServiceServicer_to_server(
        FederatedServer(), srv)
    srv.add_insecure_port(f"0.0.0.0:{port}")
    srv.start()
    print(f"\n{'='*50}\n  Serveur FL — {ip}:{port}\n{'='*50}\n")
    srv.wait_for_termination()


if __name__ == "__main__":
    demarrer_serveur()