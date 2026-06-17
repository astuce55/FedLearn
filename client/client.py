"""
client.py — Client du système d'apprentissage fédéré.

Ce que ce fichier fait (étape 1) :
  - Crée un canal gRPC vers le serveur (adresse IP + port)
  - Crée un Stub (talon) à partir du canal
  - Appelle RegisterClient() pour rejoindre le réseau
  - Affiche l'identité reçue (id plat + nom structuré)
"""
import grpc
import logging
import socket
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import federated_pb2
from common import federated_pb2_grpc

logging.basicConfig(level=logging.INFO, format="[CLIENT %(asctime)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


class FederatedClient:
    """
    Talon (stub) côté client.
    Appelle les méthodes du serveur comme si elles étaient locales.
    C'est la transparence d'accès (Van Steen ch.4).
    """

    def __init__(self, adresse_serveur, nom, dataset_size, region="yaounde"):
        self.adresse_serveur = adresse_serveur
        self.nom = nom
        self.dataset_size = dataset_size
        self.region = region

        # Identité assignée par le serveur (remplie après register())
        self.client_id = None
        self.structured_name = None

        # Étape 1 : créer le canal gRPC (connexion TCP vers le serveur)
        self.canal = grpc.insecure_channel(adresse_serveur)

        # Étape 2 : créer le stub depuis le canal
        # Le stub est généré automatiquement par grpc_tools.protoc
        self.stub = federated_pb2_grpc.FederatedLearningServiceStub(self.canal)

        log.info("Canal ouvert vers %s", adresse_serveur)

    def rejoindre_reseau(self):
        """
        Envoie un RegisterRequest au serveur et récupère l'identité assignée.
        C'est ici que le client "rejoint" officiellement le réseau.
        """
        # Construire le message (classe générée par protoc)
        requete = federated_pb2.RegisterRequest(
            client_name=self.nom,
            ip_address=socket.gethostbyname(socket.gethostname()),
            port=0,
            dataset_size=self.dataset_size,
            attributes={"region": self.region},   # nommage par attributs
        )

        # Appel RPC : ressemble à un appel de fonction normal
        # mais passe en réalité par le réseau !
        reponse = self.stub.RegisterClient(requete)

        if reponse.success:
            self.client_id = reponse.assigned_id
            self.structured_name = reponse.structured_name
            log.info("Enregistré avec succès !")
            log.info("  ID plat        : %s", self.client_id)
            log.info("  Nom structuré  : %s", self.structured_name)
            log.info("  Horloge Lamport: %d", reponse.lamport_timestamp)
        else:
            log.error("Échec de l'enregistrement !")

        return reponse


if __name__ == "__main__":
    # Usage : python -m client.client <nom> <dataset_size> <region>
    nom    = sys.argv[1] if len(sys.argv) > 1 else "client-test"
    taille = int(sys.argv[2]) if len(sys.argv) > 2 else 200
    region = sys.argv[3] if len(sys.argv) > 3 else "yaounde"

    client = FederatedClient(
        adresse_serveur="localhost:50051",
        nom=nom,
        dataset_size=taille,
        region=region
    )
    client.rejoindre_reseau()