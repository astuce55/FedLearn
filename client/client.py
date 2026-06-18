"""
client.py — Client FL réseau réel.
- Utilise GetStatus pour détecter quand un round démarre (pas de polling GetGlobalModel)
- Reste allumé indéfiniment après l'entraînement
- Bully + reconnexion automatique si le serveur tombe
"""
import grpc, logging, socket, sys, os, time, random, threading
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import federated_pb2, federated_pb2_grpc
from common.lamport_clock import HorlogeLamport
from common.bully import GestionnaireElection, TIMEOUT_HEARTBEAT, INTERVALLE_HB
from common.checkpoint import charger

PORT_LEADER_SECOURS = 50052
POLL_STATUS         = 2.0    # secondes entre chaque GetStatus


def get_ip_locale():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80)); ip = s.getsockname()[0]; s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def configurer_logs(nom):
    logging.basicConfig(
        level=logging.INFO,
        format=f"[{nom}] %(asctime)s %(levelname)s — %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )
    logging.getLogger("grpc").setLevel(logging.WARNING)
    return logging.getLogger(nom)


class FederatedClient:

    def __init__(self, adresse_serveur, nom, dataset_size,
                 region, cpu_threads=2, ram_fraction=0.5, mon_id=None):

        self.log          = configurer_logs(nom)
        self.nom          = nom
        self.dataset_size = dataset_size
        self.region       = region
        self.cpu_threads  = cpu_threads
        self.ram_fraction = ram_fraction
        self.mon_ip       = get_ip_locale()
        self.mon_id       = mon_id if mon_id is not None else random.randint(100, 9999)

        self.client_id       = None
        self.structured_name = None
        self.horloge         = HorlogeLamport(nom)

        # État
        self._actif          = True
        self._je_suis_leader = False
        self._dernier_hb_ok  = time.time()
        self._hb_thread      = None
        self._srv_secours    = None

        # Bully
        self.bully = GestionnaireElection(
            mon_id=self.mon_id,
            mon_nom=nom,
            mon_ip=self.mon_ip,
            mon_port_serveur=PORT_LEADER_SECOURS,
            callback_je_suis_leader=self._devenir_leader,
            callback_nouveau_leader=self._nouveau_leader_elu,
        )

        self._connecter(adresse_serveur)
        self.log.info("Initialisé | IP=%s | bully_id=%d | cpu=%d | ram=%.0f%%",
                      self.mon_ip, self.mon_id, cpu_threads, ram_fraction * 100)

    # ── Connexion ─────────────────────────────────────────────

    def _connecter(self, adresse):
        self._adresse = adresse
        self.canal = grpc.insecure_channel(adresse)
        self.stub  = federated_pb2_grpc.FederatedLearningServiceStub(self.canal)
        self.log.info("Canal gRPC → %s", adresse)

    # ── Enregistrement ────────────────────────────────────────

    def rejoindre_reseau(self):
        self.log.info("Connexion à %s...", self._adresse)
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
            self.log.error("Impossible de joindre %s : %s", self._adresse, e.details())
            sys.exit(1)

        self.horloge.apres_reception(rep.lamport_timestamp)
        self.client_id       = rep.assigned_id
        self.structured_name = rep.structured_name

        self.log.info("✅ Enregistré !")
        self.log.info("   ID       : %s", self.client_id)
        self.log.info("   Chemin   : %s", self.structured_name)
        self.log.info("   LC       : %d", self.horloge.valeur)
        self.log.info("⏳ En attente du lancement d'un round par le serveur...")

    # ── Heartbeat ─────────────────────────────────────────────

    def demarrer_heartbeat(self):
        if self._hb_thread and self._hb_thread.is_alive():
            return
        self._dernier_hb_ok = time.time()
        self._hb_thread = threading.Thread(
            target=self._boucle_heartbeat, daemon=True, name="heartbeat")
        self._hb_thread.start()

    def _boucle_heartbeat(self):
        while self._actif and not self._je_suis_leader:
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
                self.horloge.apres_reception(rep.lamport_timestamp)
                self._dernier_hb_ok = time.time()
                self.bully.signaler_hb_ok(0)

            except grpc.RpcError:
                delai = time.time() - self._dernier_hb_ok
                if delai >= TIMEOUT_HEARTBEAT:
                    self.log.error(
                        "❌ Serveur mort (silence %.0fs) → élection Bully", delai)
                    self.bully.signaler_hb_timeout()
                    return

    # ── GetStatus : détecte si un round est actif ─────────────

    def _get_status(self):
        """
        Interroge le serveur via GetStatus.
        Retourne StatusResponse ou None si erreur réseau.
        """
        try:
            rep = self.stub.GetStatus(
                federated_pb2.StatusRequest(), timeout=5)
            return rep
        except grpc.RpcError:
            return None

    # ── FL ────────────────────────────────────────────────────

    def get_modele_global(self):
        lc = self.horloge.avant_envoi()
        rep = self.stub.GetGlobalModel(
            federated_pb2.ModelRequest(
                client_id=self.client_id,
                lamport_timestamp=lc,
            ),
            timeout=10,
        )
        self.horloge.apres_reception(rep.lamport_timestamp)
        return list(rep.weights), rep.round_number

    def envoyer_mise_a_jour(self, poids, round_num):
        lc = self.horloge.avant_envoi()
        self.log.info("📤 Envoi update | round=%d | w=%.4f b=%.4f | LC=%d",
                      round_num, poids[0], poids[1], lc)
        try:
            rep = self.stub.SendLocalUpdate(
                federated_pb2.LocalUpdate(
                    client_id=self.client_id,
                    weights=poids,
                    dataset_size=self.dataset_size,
                    round_number=round_num,
                    lamport_timestamp=lc,
                ),
                timeout=15,
            )
            self.horloge.apres_reception(rep.lamport_timestamp)
            if rep.received:
                self.log.info("✅ Update acceptée | LC=%d", self.horloge.valeur)
            else:
                self.log.warning("⚠ Update refusée par le serveur (round terminé ou mauvais round)")
            return rep
        except grpc.RpcError as e:
            self.log.error("Erreur envoi update : %s", e.details())
            return None

    # ── Boucle principale ─────────────────────────────────────

    def boucle_fl(self, donnees):
        """
        Boucle principale du client.
        Attend que le serveur signale round_en_cours=True via GetStatus.
        S'entraîne, envoie, puis attend le round suivant.
        Client ne s'arrête JAMAIS seul (Ctrl+C pour quitter).
        """
        from client.modele_local import entrainer_local

        self.log.info("Boucle FL active. En attente des rounds du serveur...")

        dernier_round_traite = -1

        while self._actif:

            # Ne rien faire si on est le leader (on gère le serveur)
            if self._je_suis_leader:
                time.sleep(5)
                continue

            # Interroger le statut du serveur
            status = self._get_status()

            if status is None:
                # Erreur réseau → le heartbeat gère la détection de panne
                time.sleep(POLL_STATUS)
                continue

            # Pas de round en cours → attendre
            if not status.round_en_cours:
                time.sleep(POLL_STATUS)
                continue

            round_num = status.round_actuel

            # Round déjà traité → attendre le suivant
            if round_num == dernier_round_traite:
                time.sleep(POLL_STATUS)
                continue

            # ── Nouveau round détecté ! ──
            self.log.info("")
            self.log.info("🔔 Round %d démarré sur le serveur !", round_num)

            # Récupérer le modèle global
            try:
                poids_globaux, _ = self.get_modele_global()
            except grpc.RpcError as e:
                self.log.error("Erreur GetGlobalModel : %s", e.details())
                time.sleep(POLL_STATUS)
                continue

            self.log.info("   Poids reçus : w=%.4f b=%.4f", poids_globaux[0], poids_globaux[1])

            # Entraînement local
            self.log.info("🏋 Entraînement local...")
            nouveaux_poids, perte = entrainer_local(poids_globaux, donnees)
            self.log.info("   Résultat    : w=%.4f b=%.4f | perte=%.4f",
                          nouveaux_poids[0], nouveaux_poids[1], perte)

            # Envoi
            rep = self.envoyer_mise_a_jour(nouveaux_poids, round_num)
            if rep and rep.received:
                dernier_round_traite = round_num
                self.log.info("⏳ Round %d traité. En attente du prochain round...", round_num)
            else:
                self.log.warning("Update non confirmée — on réessaie au prochain cycle")

            time.sleep(1)

    # ── Callbacks Bully ───────────────────────────────────────

    def _devenir_leader(self):
        self._je_suis_leader = True
        self.log.info("")
        self.log.info("╔══════════════════════════════════════════╗")
        self.log.info("║  🏆 ÉLUUU LEADER — Démarrage du serveur  ║")
        self.log.info("╚══════════════════════════════════════════╝")

        etat = charger()
        if etat:
            self.log.info("📂 Checkpoint : round=%d | w=%.4f b=%.4f",
                          etat["round"],
                          etat["poids_globaux"][0], etat["poids_globaux"][1])
        else:
            self.log.warning("Aucun checkpoint — reprise à zéro")

        try:
            from server.server import FederatedServer, menu_serveur
            from concurrent.futures import ThreadPoolExecutor

            srv_instance = FederatedServer(nb_clients_par_round=2, reprendre=True)
            self._srv_secours_instance = srv_instance

            self._srv_secours = grpc.server(ThreadPoolExecutor(max_workers=10))
            federated_pb2_grpc.add_FederatedLearningServiceServicer_to_server(
                srv_instance, self._srv_secours)
            self._srv_secours.add_insecure_port(f"0.0.0.0:{PORT_LEADER_SECOURS}")
            self._srv_secours.start()

            adresse = f"{self.mon_ip}:{PORT_LEADER_SECOURS}"
            self.log.info("✅ Serveur de secours démarré sur %s", adresse)
            self.log.info("📢 Les autres clients vont se reconnecter à : %s", adresse)

            # Menu dans un thread séparé pour ne pas bloquer
            threading.Thread(
                target=menu_serveur,
                args=(srv_instance,),
                daemon=False,
                name="menu-leader"
            ).start()

        except Exception as e:
            self.log.error("Erreur démarrage serveur de secours : %s", e)
            import traceback; traceback.print_exc()

    def _nouveau_leader_elu(self, leader_id, leader_nom, leader_adresse):
        self.log.info("")
        self.log.info("🔄 Nouveau leader : %s (id=%d) @ %s",
                      leader_nom, leader_id, leader_adresse)
        self.log.info("⏳ Attente démarrage du nouveau serveur (3s)...")
        time.sleep(3)

        self._connecter(leader_adresse)
        self._dernier_hb_ok = time.time()
        self.client_id = None

        try:
            self.rejoindre_reseau()
            self.demarrer_heartbeat()
            self.log.info("✅ Reconnecté au nouveau leader !")
        except Exception as e:
            self.log.error("Échec reconnexion : %s", e)


# ── Point d'entrée ────────────────────────────────────────────

def menu_interactif(adresse_serveur=None):
    print("\n" + "="*55)
    print("   Apprentissage Fédéré Distribué — Client")
    print("="*55)
    if not adresse_serveur:
        adresse_serveur = input(
            "\nAdresse du serveur (ex: 192.168.1.10:50051) : ").strip() or "localhost:50051"
    nom          = input("Ton prénom/pseudo : ").strip() or "anonyme"
    region       = input("Région (ex: yaounde) : ").strip() or "yaounde"
    dataset_size = int(input("Taille dataset [200] : ").strip() or "200")
    print("\n-- Ressources --")
    cpu  = int(input("Threads CPU [2] : ").strip() or "2")
    ram  = float(input("Fraction RAM 0.1-1.0 [0.5] : ").strip() or "0.5")
    return adresse_serveur, nom, dataset_size, region, cpu, ram


if __name__ == "__main__":
    from client.modele_local import generer_donnees_locales

    adresse = sys.argv[1] if len(sys.argv) > 1 else None
    adresse, nom, dataset_size, region, cpu, ram = menu_interactif(adresse)

    client = FederatedClient(
        adresse_serveur=adresse, nom=nom,
        dataset_size=dataset_size, region=region,
        cpu_threads=cpu, ram_fraction=ram,
    )

    client.rejoindre_reseau()
    client.demarrer_heartbeat()

    seed    = random.randint(0, 9999)
    donnees = generer_donnees_locales(dataset_size, seed=seed)
    client.log.info("📊 Données locales : %d exemples (seed=%d)", dataset_size, seed)

    try:
        client.boucle_fl(donnees)
    except KeyboardInterrupt:
        client._actif = False
        client.log.info("Arrêt.")