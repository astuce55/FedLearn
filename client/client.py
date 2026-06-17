"""
client.py — Version réseau réel.
Heartbeat, Bully, devient serveur si élu, logs clairs.
"""
import grpc, logging, socket, sys, os, time, random, threading
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import federated_pb2, federated_pb2_grpc
from common.lamport_clock import HorlogeLamport
from common.bully import GestionnaireElection, TIMEOUT_HEARTBEAT, INTERVALLE_HB
from common.checkpoint import charger

PORT_LEADER_SECOURS = 50052

def get_ip_locale():
    """Retourne l'IP locale sur le réseau (pas 127.0.0.1)."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def configurer_logs(nom):
    """Configure des logs bien lisibles avec le nom du processus."""
    fmt = f"[{nom}] %(asctime)s %(levelname)s — %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    # Réduire le bruit de gRPC
    logging.getLogger("grpc").setLevel(logging.WARNING)
    return logging.getLogger(nom)


class FederatedClient:

    def __init__(self, adresse_serveur, nom, dataset_size,
                 region, cpu_threads=2, ram_fraction=0.5, mon_id=None):

        self.log = configurer_logs(nom)
        self.nom = nom
        self.dataset_size = dataset_size
        self.region = region
        self.cpu_threads = cpu_threads
        self.ram_fraction = ram_fraction
        self.adresse_serveur = adresse_serveur
        self.mon_ip = get_ip_locale()

        # ID Bully : plus grand = prioritaire pour l'élection
        self.mon_id = mon_id if mon_id is not None else random.randint(1, 999)

        self.client_id = None
        self.structured_name = None
        self.horloge = HorlogeLamport(nom)

        # Connexion initiale au serveur
        self._connecter(adresse_serveur)

        # Bully : gestionnaire d'élection
        self.bully = GestionnaireElection(
            mon_id=self.mon_id,
            mon_nom=self.nom,
            mon_ip=self.mon_ip,
            mon_port_serveur=PORT_LEADER_SECOURS,
            callback_je_suis_leader=self._devenir_leader,
            callback_nouveau_leader=self._nouveau_leader_elu,
        )

        self._je_suis_leader   = False
        self._serveur_secours  = None
        self._dernier_hb_ok    = time.time()

        self.log.info("Initialisé | IP=%s | bully_id=%d | cpu=%d | ram=%.0f%%",
                      self.mon_ip, self.mon_id, cpu_threads, ram_fraction * 100)

    # ── Connexion ─────────────────────────────────────────────

    def _connecter(self, adresse):
        self.adresse_serveur = adresse
        self.canal = grpc.insecure_channel(adresse)
        self.stub  = federated_pb2_grpc.FederatedLearningServiceStub(self.canal)
        self.log.info("Canal gRPC ouvert → %s", adresse)

    # ── Enregistrement ────────────────────────────────────────

    def rejoindre_reseau(self):
        self.log.info("Envoi RegisterClient au serveur %s...", self.adresse_serveur)
        lc = self.horloge.avant_envoi()

        req = federated_pb2.RegisterRequest(
            client_name=self.nom,
            ip_address=self.mon_ip,
            port=PORT_LEADER_SECOURS,
            dataset_size=self.dataset_size,
            attributes={
                "region":       self.region,
                "cpu_threads":  str(self.cpu_threads),
                "ram_fraction": str(self.ram_fraction),
                "bully_id":     str(self.mon_id),
                "ip":           self.mon_ip,
            },
            lamport_timestamp=lc,
        )

        try:
            rep = self.stub.RegisterClient(req, timeout=10)
        except grpc.RpcError as e:
            self.log.error("Impossible de joindre le serveur %s : %s",
                           self.adresse_serveur, e.details())
            sys.exit(1)

        self.horloge.apres_reception(rep.lamport_timestamp)
        self.client_id = rep.assigned_id
        self.structured_name = rep.structured_name

        self.log.info("✅ Enregistré avec succès !")
        self.log.info("   ID plat       : %s", self.client_id)
        self.log.info("   Nom structuré : %s", self.structured_name)
        self.log.info("   Horloge LC    : %d", self.horloge.valeur)

        # Démarrer la surveillance heartbeat
        self._demarrer_heartbeat()
        return rep

    # ── Heartbeat ─────────────────────────────────────────────

    def _demarrer_heartbeat(self):
        t = threading.Thread(target=self._boucle_heartbeat, daemon=True)
        t.start()
        self.log.info("Heartbeat démarré (toutes les %.0fs, timeout=%.0fs)",
                      INTERVALLE_HB, TIMEOUT_HEARTBEAT)

    def _boucle_heartbeat(self):
        while not self._je_suis_leader:
            time.sleep(INTERVALLE_HB)
            try:
                lc = self.horloge.avant_envoi()
                rep = self.stub.Heartbeat(
                    federated_pb2.HeartbeatRequest(
                        sender_id=self.client_id or self.nom,
                        lamport_timestamp=lc,
                    ),
                    timeout=3,
                )
                if rep.alive:
                    self.horloge.apres_reception(rep.lamport_timestamp)
                    self._dernier_hb_ok = time.time()
                    self.bully.signaler_hb_ok(0)

            except grpc.RpcError:
                delai = time.time() - self._dernier_hb_ok
                self.log.warning("⚠ Heartbeat échoué (serveur muet depuis %.1fs)", delai)

                if delai >= TIMEOUT_HEARTBEAT:
                    self.log.error("❌ Serveur considéré MORT (timeout=%.0fs) !", TIMEOUT_HEARTBEAT)
                    self.log.info("🗳  Lancement de l'élection Bully...")
                    self.bully.signaler_hb_timeout()
                    break

    # ── Callbacks Bully ───────────────────────────────────────

    def _devenir_leader(self):
        """Je gagne l'élection → je démarre un serveur gRPC sur ma machine."""
        self._je_suis_leader = True

        self.log.info("")
        self.log.info("╔══════════════════════════════════════╗")
        self.log.info("║  🏆 ÉLUUU LEADER — Démarrage serveur ║")
        self.log.info("╚══════════════════════════════════════╝")

        # Charger l'état depuis le checkpoint
        etat = charger()
        if etat:
            self.log.info("📂 Checkpoint chargé (round=%d, poids=%s)",
                          etat["round"], [round(p,4) for p in etat["poids_globaux"]])
        else:
            self.log.warning("Pas de checkpoint — reprise à zéro")

        # Démarrer le serveur de secours
        from server.server import FederatedServer
        from concurrent.futures import ThreadPoolExecutor

        srv_instance = FederatedServer(reprendre_depuis_checkpoint=True)
        self._serveur_secours = grpc.server(ThreadPoolExecutor(max_workers=10))
        federated_pb2_grpc.add_FederatedLearningServiceServicer_to_server(
            srv_instance, self._serveur_secours)
        self._serveur_secours.add_insecure_port(f"0.0.0.0:{PORT_LEADER_SECOURS}")
        self._serveur_secours.start()

        adresse = f"{self.mon_ip}:{PORT_LEADER_SECOURS}"
        self.log.info("✅ Serveur de secours démarré sur %s", adresse)
        self.log.info("📢 Les autres clients doivent se reconnecter à : %s", adresse)

    def _nouveau_leader_elu(self, leader_id, leader_nom, leader_adresse):
        """Un autre client a gagné → je me reconnecte à lui."""
        self.log.info("")
        self.log.info("🔄 Nouveau leader : %s (id=%d) @ %s", leader_nom, leader_id, leader_adresse)
        self.log.info("🔌 Reconnexion au nouveau leader...")

        time.sleep(2)   # laisser le nouveau leader démarrer son serveur
        self._connecter(leader_adresse)

        # Se ré-enregistrer auprès du nouveau leader
        try:
            self.rejoindre_reseau()
            self.log.info("✅ Reconnecté au nouveau leader !")
        except Exception as e:
            self.log.error("Échec reconnexion : %s", e)

    # ── FL ────────────────────────────────────────────────────

    def get_modele_global(self):
        lc = self.horloge.avant_envoi()
        rep = self.stub.GetGlobalModel(
            federated_pb2.ModelRequest(client_id=self.client_id, lamport_timestamp=lc),
            timeout=10,
        )
        self.horloge.apres_reception(rep.lamport_timestamp)
        self.log.info("Modèle global reçu | round=%d | w=%.4f b=%.4f | LC=%d",
                      rep.round_number, rep.weights[0], rep.weights[1], self.horloge.valeur)
        return list(rep.weights)

    def envoyer_mise_a_jour(self, poids, round_num):
        lc = self.horloge.avant_envoi()
        self.log.info("Envoi update | round=%d | w=%.4f b=%.4f | LC=%d",
                      round_num, poids[0], poids[1], lc)
        rep = self.stub.SendLocalUpdate(
            federated_pb2.LocalUpdate(
                client_id=self.client_id, weights=poids,
                dataset_size=self.dataset_size,
                round_number=round_num,
                lamport_timestamp=lc,
            ),
            timeout=10,
        )
        self.horloge.apres_reception(rep.lamport_timestamp)
        self.log.info("ACK reçu | LC=%d", self.horloge.valeur)
        return rep


def menu_interactif(adresse_serveur=None):
    """Menu pour les camarades — saisie de leurs paramètres."""
    print("\n" + "="*55)
    print("   Système d'apprentissage fédéré distribué")
    print("="*55)

    if not adresse_serveur:
        adresse_serveur = input("\nAdresse du serveur (ex: 192.168.1.10:50051) : ").strip()
        if not adresse_serveur:
            adresse_serveur = "localhost:50051"

    nom          = input("Ton prénom/pseudo : ").strip() or "anonyme"
    region       = input("Ta région (ex: yaounde) : ").strip() or "yaounde"
    dataset_size = int(input("Taille dataset local [200] : ").strip() or "200")

    print("\n-- Ressources allouées au calcul --")
    cpu  = int(input("Threads CPU (1/2/4/8) [2] : ").strip() or "2")
    ram  = float(input("Fraction RAM (0.1 à 1.0) [0.5] : ").strip() or "0.5")

    return adresse_serveur, nom, dataset_size, region, cpu, ram


if __name__ == "__main__":
    from client.modele_local import generer_donnees_locales, entrainer_local

    # Adresse passée en argument ou saisie
    adresse = sys.argv[1] if len(sys.argv) > 1 else None
    adresse, nom, dataset_size, region, cpu, ram = menu_interactif(adresse)

    client = FederatedClient(
        adresse_serveur=adresse,
        nom=nom,
        dataset_size=dataset_size,
        region=region,
        cpu_threads=cpu,
        ram_fraction=ram,
    )

    client.rejoindre_reseau()

    # Générer les données locales une fois
    seed = random.randint(0, 999)
    donnees = generer_donnees_locales(dataset_size, seed=seed)
    client.log.info("Données locales prêtes : %d exemples (seed=%d)", dataset_size, seed)

    # Boucle FL — continue jusqu'à Ctrl+C
    round_num = 1
    client.log.info("Démarrage de la boucle FL. Ctrl+C pour arrêter.")
    try:
        while True:
            # Attendre si on est en élection
            if client.bully._en_election:
                client.log.info("Élection en cours, pause...")
                time.sleep(2)
                continue

            try:
                client.log.info("─── Round %d ───", round_num)
                poids = client.get_modele_global()
                nouveaux, perte = entrainer_local(poids, donnees)
                client.log.info("Entraînement local | perte=%.4f | w=%.4f b=%.4f",
                                perte, nouveaux[0], nouveaux[1])
                client.envoyer_mise_a_jour(nouveaux, round_num)
                round_num += 1
                time.sleep(1)

            except grpc.RpcError as e:
                client.log.warning("Erreur gRPC : %s — attente reconnexion...", e.details())
                time.sleep(3)

    except KeyboardInterrupt:
        client.log.info("Arrêt du client.")