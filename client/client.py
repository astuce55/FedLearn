"""
client.py — Client du système d'apprentissage fédéré.
"""
import grpc
import logging
import socket
import sys
import os
import time
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import federated_pb2
from common import federated_pb2_grpc

logging.basicConfig(level=logging.INFO, format="[%(name)s %(asctime)s] %(message)s", datefmt="%H:%M:%S")


class FederatedClient:
    def __init__(self, adresse_serveur, nom, dataset_size, region,
                 cpu_threads, ram_fraction):
        """
        adresse_serveur : "192.168.1.10:50051"  (IP du serveur + port)
        nom             : prénom/pseudo du camarade
        dataset_size    : taille simulée du dataset local
        region          : localisation (ex: "yaounde")
        cpu_threads     : nb de threads CPU alloués (1 à N)
        ram_fraction    : fraction de RAM allouée (0.1 à 1.0)
        """
        self.log = logging.getLogger(nom)
        self.adresse_serveur = adresse_serveur
        self.nom = nom
        self.dataset_size = dataset_size
        self.region = region
        self.cpu_threads = cpu_threads
        self.ram_fraction = ram_fraction

        # Calculé à partir des ressources allouées
        # Simule le temps d'entraînement local : moins de ressources = plus lent
        self.vitesse_calcul = cpu_threads * ram_fraction

        self.client_id = None
        self.structured_name = None

        self.canal = grpc.insecure_channel(adresse_serveur)
        self.stub = federated_pb2_grpc.FederatedLearningServiceStub(self.canal)

        self.log.info("Canal ouvert vers %s | CPU: %d threads | RAM: %.0f%%",
                      adresse_serveur, cpu_threads, ram_fraction * 100)

    def rejoindre_reseau(self):
        """Enregistrement auprès du serveur."""
        requete = federated_pb2.RegisterRequest(
            client_name=self.nom,
            ip_address=socket.gethostbyname(socket.gethostname()),
            port=0,
            dataset_size=self.dataset_size,
            attributes={
                "region": self.region,
                "cpu_threads": str(self.cpu_threads),
                "ram_fraction": str(self.ram_fraction),
            }
        )
        rep = self.stub.RegisterClient(requete)
        if rep.success:
            self.client_id = rep.assigned_id
            self.structured_name = rep.structured_name
            self.log.info("Enregistre ! ID=%s | Chemin=%s",
                          self.client_id, self.structured_name)
        return rep

    def simuler_entrainement_local(self, poids_globaux, round_num):
        """
        Simule un entraînement local.
        Le temps de calcul dépend des ressources allouées par le camarade.
        """
        # Temps simulé : inversement proportionnel aux ressources allouées
        temps_base = 2.0  # secondes pour une machine pleine puissance
        temps_reel = temps_base / self.vitesse_calcul

        self.log.info(
            "Round %d — Debut entrainement local (simulation %.1fs avec %d threads, %.0f%% RAM)...",
            round_num, temps_reel, self.cpu_threads, self.ram_fraction * 100
        )
        time.sleep(temps_reel)

        # Simuler une légère amélioration des poids
        import random
        nouveaux_poids = [w + random.uniform(-0.05, 0.05) for w in poids_globaux]
        self.log.info("Round %d — Entrainement termine. Poids mis a jour.", round_num)
        return nouveaux_poids

    def envoyer_mise_a_jour(self, poids, round_num):
        """Envoie les poids locaux au serveur."""
        req = federated_pb2.LocalUpdate(
            client_id=self.client_id,
            weights=poids,
            dataset_size=self.dataset_size,
            round_number=round_num,
            lamport_timestamp=0,
        )
        rep = self.stub.SendLocalUpdate(req)
        self.log.info("Round %d — Mise a jour envoyee au serveur (ack=%s)", round_num, rep.received)
        return rep


def afficher_menu_ressources():
    """Menu interactif pour que le camarade choisisse ses ressources."""
    print("\n" + "="*50)
    print("  Bienvenue dans le reseau d'apprentissage federe")
    print("="*50)

    nom = input("\nTon prenom/pseudo : ").strip() or "anonyme"
    region = input("Ta region (ex: yaounde, douala...) : ").strip() or "yaounde"
    dataset_size = int(input("Taille de ton dataset local (ex: 100 a 1000) : ") or "200")

    print("\n--- Ressources a allouer au calcul ---")
    print("(Plus tu alloues, plus ton entrainement est rapide)")

    cpu = int(input("Nombre de threads CPU (1, 2, 4, 8) [defaut: 2] : ") or "2")
    ram = float(input("Fraction de RAM (0.1 = 10%%, 0.5 = 50%%, 1.0 = 100%%) [defaut: 0.5] : ") or "0.5")

    ram = max(0.1, min(1.0, ram))
    cpu = max(1, min(16, cpu))

    print(f"\nConfiguration : {nom} | {cpu} threads | {ram*100:.0f}% RAM | {dataset_size} exemples")
    confirm = input("Confirmer ? (o/n) : ").strip().lower()
    if confirm != 'o':
        print("Annule.")
        sys.exit(0)

    return nom, region, dataset_size, cpu, ram


if __name__ == "__main__":
    # Adresse du serveur passée en argument, sinon demandée
    if len(sys.argv) > 1:
        adresse = sys.argv[1]
    else:
        adresse = input("\nAdresse du serveur (ex: 192.168.1.10:50051) : ").strip()
        if not adresse:
            adresse = "localhost:50051"

    nom, region, dataset_size, cpu, ram = afficher_menu_ressources()

    client = FederatedClient(
        adresse_serveur=adresse,
        nom=nom,
        dataset_size=dataset_size,
        region=region,
        cpu_threads=cpu,
        ram_fraction=ram,
    )

    client.rejoindre_reseau()

    # Simuler 2 rounds de FL pour la démo
    for round_num in range(1, 3):
        # 1. Récupérer le modèle global
        rep = client.stub.GetGlobalModel(
            federated_pb2.ModelRequest(client_id=client.client_id, lamport_timestamp=0)
        )
        poids_globaux = list(rep.weights)

        # 2. Entraîner localement
        nouveaux_poids = client.simuler_entrainement_local(poids_globaux, round_num)

        # 3. Envoyer la mise à jour
        client.envoyer_mise_a_jour(nouveaux_poids, round_num)

    print("\nParticipation terminee. Merci !")