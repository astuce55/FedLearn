"""
client.py — Étape 3 : avec horloges de Lamport.
"""
import grpc, logging, socket, sys, os, time, random
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import federated_pb2, federated_pb2_grpc
from common.lamport_clock import HorlogeLamport, JournalEvenements

logging.basicConfig(level=logging.INFO, format="[%(name)s %(asctime)s] %(message)s", datefmt="%H:%M:%S")


class FederatedClient:

    def __init__(self, adresse_serveur, nom, dataset_size, region,
                 cpu_threads=2, ram_fraction=0.5):
        self.log = logging.getLogger(nom)
        self.nom = nom
        self.dataset_size = dataset_size
        self.region = region
        self.cpu_threads = cpu_threads
        self.ram_fraction = ram_fraction
        self.vitesse = cpu_threads * ram_fraction

        self.client_id = None
        self.structured_name = None

        self.horloge = HorlogeLamport(nom)     # ← horloge propre à ce client
        self.journal = JournalEvenements()

        self.canal = grpc.insecure_channel(adresse_serveur)
        self.stub  = federated_pb2_grpc.FederatedLearningServiceStub(self.canal)

    def rejoindre_reseau(self):
        # Règle 1 : événement interne (préparation de la requête)
        lc = self.horloge.evenement_local()
        self.journal.enregistrer(lc, self.nom, "Prépare RegisterRequest", "")

        # Règle 2 : envoi
        lc_envoi = self.horloge.avant_envoi()
        self.journal.enregistrer(lc_envoi, self.nom, "RegisterClient envoyé", f"LC={lc_envoi}")

        req = federated_pb2.RegisterRequest(
            client_name=self.nom,
            ip_address=socket.gethostbyname(socket.gethostname()),
            port=0, dataset_size=self.dataset_size,
            attributes={"region": self.region,
                        "cpu_threads": str(self.cpu_threads),
                        "ram_fraction": str(self.ram_fraction)},
            lamport_timestamp=lc_envoi,        # ← estampille le message
        )
        rep = self.stub.RegisterClient(req)

        # Règle 3 : réception de la réponse du serveur
        lc_rep = self.horloge.apres_reception(rep.lamport_timestamp)
        self.journal.enregistrer(lc_rep, self.nom, "RegisterResponse reçu",
                                 f"id={rep.assigned_id} LC_srv={rep.lamport_timestamp}")

        self.client_id = rep.assigned_id
        self.structured_name = rep.structured_name
        self.log.info("Enregistré | id=%s | LC local=%d (synchronisé avec serveur)",
                      self.client_id, lc_rep)
        return rep

    def simuler_entrainement(self, poids_globaux, round_num):
        # Règle 1 : chaque itération d'entraînement = événement interne
        temps = round(2.0 / self.vitesse, 2)
        self.log.info("Round %d — entraînement (%.1fs simulées)...", round_num, temps)
        time.sleep(min(temps, 1.0))   # plafonné à 1s pour la démo

        lc = self.horloge.evenement_local()
        self.journal.enregistrer(lc, self.nom, f"Entraînement terminé", f"round={round_num} LC={lc}")

        return [w + random.uniform(-0.05, 0.05) for w in poids_globaux]

    def envoyer_mise_a_jour(self, poids, round_num):
        # Règle 2 : envoi
        lc_envoi = self.horloge.avant_envoi()
        self.journal.enregistrer(lc_envoi, self.nom, "SendLocalUpdate envoyé",
                                 f"round={round_num} LC={lc_envoi}")

        rep = self.stub.SendLocalUpdate(federated_pb2.LocalUpdate(
            client_id=self.client_id, weights=poids,
            dataset_size=self.dataset_size, round_number=round_num,
            lamport_timestamp=lc_envoi,
        ))

        # Règle 3 : réception de l'ACK
        lc_ack = self.horloge.apres_reception(rep.lamport_timestamp)
        self.journal.enregistrer(lc_ack, self.nom, "ACK reçu", f"LC={lc_ack}")
        return rep

    def get_modele_global(self):
        lc_envoi = self.horloge.avant_envoi()
        rep = self.stub.GetGlobalModel(federated_pb2.ModelRequest(
            client_id=self.client_id,
            lamport_timestamp=lc_envoi,
        ))
        lc = self.horloge.apres_reception(rep.lamport_timestamp)
        self.journal.enregistrer(lc, self.nom, "Modèle global reçu", f"LC={lc}")
        return list(rep.weights)