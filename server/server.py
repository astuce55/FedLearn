"""
server.py — Serveur FL avec menu de contrôle.
- L'entraînement ne démarre JAMAIS automatiquement
- Le menu permet de lancer les rounds, voir les clients, etc.
- Compatible avec le proto mis à jour (UpdateAck.received, StartRound, GetStatus)
"""
import grpc, socket, logging, threading, time, sys, os
from concurrent import futures
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import federated_pb2, federated_pb2_grpc
from common.naming import AnnuaireUnifie
from common.lamport_clock import HorlogeLamport, JournalEvenements
from common.checkpoint import sauvegarder, charger
from server.aggregator import AggregateurFedAvg

logging.basicConfig(
    level=logging.INFO,
    format="[SERVEUR %(asctime)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logging.getLogger("grpc").setLevel(logging.WARNING)
log = logging.getLogger("SERVEUR")

TAILLE_MODEL = 2


class FederatedServer(federated_pb2_grpc.FederatedLearningServiceServicer):

    def __init__(self, nb_clients_par_round=2, reprendre=True):
        self.annuaire    = AnnuaireUnifie()
        self.horloge     = HorlogeLamport("serveur")
        self.journal     = JournalEvenements()
        self._lock       = threading.Lock()

        self._entrainement_en_cours = False
        self._round_actuel          = 0
        self._nb_clients_round      = nb_clients_par_round

        self.aggregateur = AggregateurFedAvg(nb_clients_par_round, TAILLE_MODEL)

        if reprendre:
            etat = charger()
            if etat:
                self.aggregateur.poids_globaux = etat["poids_globaux"]
                self._round_actuel             = etat["round"]
                self.horloge.apres_reception(etat.get("lamport_clock", 0))
                log.info("══ REPRISE checkpoint round=%d poids=w=%.4f b=%.4f ══",
                         self._round_actuel,
                         etat["poids_globaux"][0], etat["poids_globaux"][1])
            else:
                log.info("Démarrage à zéro.")

    # ── Propriétés ────────────────────────────────────────────

    @property
    def nb_clients_connectes(self):
        return len(self.annuaire._clients)

    def get_info_clients(self):
        return dict(self.annuaire._clients)

    # ── RPC RegisterClient ────────────────────────────────────

    def RegisterClient(self, request, context):
        lc = self.horloge.apres_reception(request.lamport_timestamp)

        peer = context.peer()
        ip   = peer.split(":")[1] if "ipv4:" in peer else request.ip_address
        attrs = {k: v for k, v in request.attributes.items() if k != "region"}

        res = self.annuaire.enregistrer_client(
            nom=request.client_name,
            ip=ip, port=request.port,
            region=request.attributes.get("region", "default"),
            dataset_size=request.dataset_size,
            attrs_supplementaires=attrs,
        )
        lc_rep = self.horloge.avant_envoi()

        statut = "EN ATTENTE (round en cours)" if self._entrainement_en_cours else "PRÊT"
        log.info("✅ Client connecté | %s | id=%s | n=%d | statut=%s | total=%d",
                 request.client_name, res["client_id"],
                 request.dataset_size, statut, self.nb_clients_connectes)

        return federated_pb2.RegisterResponse(
            success=True,
            assigned_id=res["client_id"],
            structured_name=res["chemin"],
            lamport_timestamp=lc_rep,
        )

    # ── RPC GetGlobalModel ────────────────────────────────────

    def GetGlobalModel(self, request, context):
        lc = self.horloge.apres_reception(request.lamport_timestamp)
        lc_rep = self.horloge.avant_envoi()
        with self._lock:
            poids   = list(self.aggregateur.poids_globaux)
            round_n = self._round_actuel
        return federated_pb2.ModelResponse(
            weights=poids,
            round_number=round_n,
            lamport_timestamp=lc_rep,
        )

    # ── RPC GetStatus ─────────────────────────────────────────

    def GetStatus(self, request, context):
        """
        Le client appelle GetStatus pour savoir si un round est actif.
        C'est le seul moyen propre de détecter le démarrage d'un round.
        """
        with self._lock:
            en_cours   = self._entrainement_en_cours
            round_n    = self._round_actuel
            nb         = self.nb_clients_connectes
            poids      = list(self.aggregateur.poids_globaux)
        return federated_pb2.StatusResponse(
            nb_clients=nb,
            round_actuel=round_n,
            round_en_cours=en_cours,
            poids=poids,
        )

    # ── RPC SendLocalUpdate ───────────────────────────────────

    def SendLocalUpdate(self, request, context):
        lc = self.horloge.apres_reception(request.lamport_timestamp)

        with self._lock:
            en_cours      = self._entrainement_en_cours
            round_actuel  = self._round_actuel

        info = self.annuaire.get_client(request.client_id)
        nom  = info["name"] if info else "?"

        # Ignorer si pas d'entraînement en cours ou mauvais round
        if not en_cours or request.round_number != round_actuel:
            lc_rep = self.horloge.avant_envoi()
            log.warning("⚠ Update ignorée de %s (round=%d attendu=%d en_cours=%s)",
                        nom, request.round_number, round_actuel, en_cours)
            return federated_pb2.UpdateAck(received=False, lamport_timestamp=lc_rep)

        log.info("📨 Update reçue de %s | round=%d | w=%.4f b=%.4f",
                 nom, request.round_number,
                 request.weights[0], request.weights[1])

        complet = self.aggregateur.ajouter_update(
            client_id=request.client_id,
            poids=list(request.weights),
            n_k=request.dataset_size,
            lamport_ts=request.lamport_timestamp,
        )

        if complet:
            nouveaux_poids = self.aggregateur.agreger()

            with self._lock:
                self._entrainement_en_cours = False
                self._round_actuel         += 1

            # Checkpoint
            clients_snap = {
                cid: {"name": c["name"], "region": c.get("region","?")}
                for cid, c in self.annuaire._clients.items()
            }
            sauvegarder(
                round_num=self._round_actuel - 1,
                poids_globaux=nouveaux_poids,
                clients=clients_snap,
                lamport_clock=self.horloge.valeur,
            )

            log.info("")
            log.info("╔══════════════════════════════════════════════╗")
            log.info("║  ✅ FedAvg round %d terminé                   ║",
                     self._round_actuel - 1)
            log.info("║     w=%.4f    b=%.4f                      ║",
                     nouveaux_poids[0], nouveaux_poids[1])
            log.info("║     Checkpoint sauvegardé                    ║")
            log.info("╚══════════════════════════════════════════════╝")
            log.info("")
            log.info(">>> Revenir au menu serveur pour lancer le prochain round.")

            self.aggregateur.demarrer_round()

        lc_rep = self.horloge.avant_envoi()
        return federated_pb2.UpdateAck(received=True, lamport_timestamp=lc_rep)

    # ── RPC StartRound ────────────────────────────────────────

    def StartRound(self, request, context):
        """
        Appelé par le menu serveur pour démarrer un round.
        N'est PAS appelé automatiquement — seulement via le menu.
        """
        lc = self.horloge.apres_reception(request.lamport_timestamp)
        with self._lock:
            en_cours = self._entrainement_en_cours
            round_n  = self._round_actuel
        lc_rep = self.horloge.avant_envoi()
        return federated_pb2.StartRoundResponse(
            round_number=round_n,
            lamport_timestamp=lc_rep,
        )

    # ── RPC RemoveClient ──────────────────────────────────────

    def RemoveClient(self, request, context):
        info = self.annuaire.get_client(request.client_id)
        if info:
            self.annuaire.deconnecter_client(request.client_id)
            log.info("🗑  Client supprimé : %s (%s)", info["name"], request.client_id)
            return federated_pb2.RemoveClientResponse(success=True)
        return federated_pb2.RemoveClientResponse(success=False)

    # ── RPC Heartbeat ─────────────────────────────────────────

    def Heartbeat(self, request, context):
        lc = self.horloge.apres_reception(request.lamport_timestamp)
        lc_rep = self.horloge.avant_envoi()
        return federated_pb2.HeartbeatResponse(alive=True, lamport_timestamp=lc_rep)

    # ── RPC ElectionMessage ───────────────────────────────────

    def ElectionMessage(self, request, context):
        lc = self.horloge.apres_reception(request.lamport_timestamp)
        cid = request.candidate_id
        if cid.startswith("COORD:"):
            parts = cid.split(":")
            log.info("🏆 COORDINATEUR : nouveau leader = %s (id=%s)",
                     parts[2] if len(parts) > 2 else "?", parts[1])
        lc_rep = self.horloge.avant_envoi()
        return federated_pb2.ElectionResponse(accepted=True, lamport_timestamp=lc_rep)

    # ── Méthodes appelées par le menu ─────────────────────────

    def lancer_round(self, nb_clients: int) -> bool:
        with self._lock:
            if self._entrainement_en_cours:
                log.warning("Un entraînement est déjà en cours !")
                return False
            if self.nb_clients_connectes < nb_clients:
                log.warning("Pas assez de clients (%d connectés, %d requis)",
                            self.nb_clients_connectes, nb_clients)
                return False
            self._entrainement_en_cours        = True
            self.aggregateur.nb_clients_attendus = nb_clients
            self.aggregateur.demarrer_round()

        log.info("")
        log.info("🚀 Round %d lancé — attente de %d client(s)",
                 self._round_actuel, nb_clients)
        return True

    def supprimer_client(self, client_id: str) -> bool:
        info = self.annuaire.get_client(client_id)
        if not info:
            return False
        self.annuaire.deconnecter_client(client_id)
        log.info("🗑  Supprimé : %s (%s)", info["name"], client_id)
        return True


# ── Menu de contrôle ──────────────────────────────────────────

def menu_serveur(srv_instance: FederatedServer):
    while True:
        print("\n" + "─"*45)
        print("  MENU SERVEUR")
        print("─"*45)
        print("  1. Voir les clients connectés")
        print("  2. Lancer un round d'entraînement")
        print("  3. Historique des rounds")
        print("  4. Supprimer un client")
        print("  5. État du checkpoint")
        print("  0. Arrêter le serveur")
        print("─"*45)

        try:
            choix = input("  > ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nArrêt.")
            break

        if choix == "0":
            break

        elif choix == "1":
            clients = srv_instance.get_info_clients()
            if not clients:
                print("  Aucun client.")
            else:
                print(f"\n  {len(clients)} client(s) connecté(s) :\n")
                for cid, info in clients.items():
                    attrs = info.get("attributes", {})
                    print(f"  [{cid[:8]}] {info['name']}")
                    print(f"     IP      : {info.get('ip','?')}")
                    print(f"     Région  : {info.get('region','?')}")
                    print(f"     Dataset : {info['dataset_size']} exemples")
                    print(f"     CPU     : {attrs.get('cpu_threads','?')} threads")
                    print(f"     RAM     : {attrs.get('ram_fraction','?')}")
                    print(f"     Bully ID: {attrs.get('bully_id','?')}")

        elif choix == "2":
            nb = srv_instance.nb_clients_connectes
            if nb == 0:
                print("  Aucun client connecté.")
                continue
            print(f"  {nb} client(s) disponible(s).")
            try:
                rep = input(f"  Combien participer à ce round ? [1-{nb}] : ").strip()
                n = int(rep) if rep else nb
                n = max(1, min(n, nb))
            except ValueError:
                n = nb
            ok = srv_instance.lancer_round(n)
            if ok:
                print(f"  ✅ Round {srv_instance._round_actuel} lancé ({n} clients).")

        elif choix == "3":
            hist = srv_instance.aggregateur.get_historique()
            if not hist:
                print("  Aucun round terminé.")
            else:
                print(f"\n  {len(hist)} round(s) :\n")
                for h in hist:
                    print(f"  Round {h['round']} → "
                          f"w={h['poids'][0]:.4f} b={h['poids'][1]:.4f} | "
                          f"{h['clients']} clients | N={h['N']}")

        elif choix == "4":
            clients = srv_instance.get_info_clients()
            if not clients:
                print("  Aucun client.")
                continue
            for cid, info in clients.items():
                print(f"  [{cid[:8]}] {info['name']}")
            cid_full = None
            partiel = input("  ID (8 premiers caractères) : ").strip()
            for cid in clients:
                if cid.startswith(partiel):
                    cid_full = cid
                    break
            if cid_full and srv_instance.supprimer_client(cid_full):
                print(f"  ✅ Client supprimé.")
            else:
                print(f"  ❌ Introuvable.")

        elif choix == "5":
            etat = charger()
            if not etat:
                print("  Aucun checkpoint.")
            else:
                print(f"\n  Round      : {etat['round']}")
                print(f"  Poids      : w={etat['poids_globaux'][0]:.4f}"
                      f" b={etat['poids_globaux'][1]:.4f}")
                print(f"  Horloge LC : {etat['lamport_clock']}")
                print(f"  Clients    : {len(etat['clients'])}")
                print(f"  Sauvegardé : {etat.get('timestamp_lisible','?')}")


# ── Point d'entrée ────────────────────────────────────────────

def get_ip_locale():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80)); ip = s.getsockname()[0]; s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def demarrer_serveur(port=50051, reprendre=True, nb_clients_par_round=2):
    ip = get_ip_locale()
    srv_instance = FederatedServer(
        nb_clients_par_round=nb_clients_par_round, reprendre=reprendre)
    srv = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    federated_pb2_grpc.add_FederatedLearningServiceServicer_to_server(srv_instance, srv)
    srv.add_insecure_port(f"0.0.0.0:{port}")
    srv.start()
    print("")
    print("╔" + "═"*50 + "╗")
    print("║  Serveur FL démarré !                            ║")
    print(f"║  Adresse → {ip}:{port:<37}║")
    if srv_instance._round_actuel > 0:
        print(f"║  Reprise depuis round {srv_instance._round_actuel:<28}║")
    print("╚" + "═"*50 + "╝")
    return srv, srv_instance


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 50051
    srv, srv_instance = demarrer_serveur(port=port)
    try:
        menu_serveur(srv_instance)
    except KeyboardInterrupt:
        pass
    finally:
        srv.stop(0)
        print("Serveur arrêté.")