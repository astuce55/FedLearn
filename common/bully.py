"""
common/bully.py — Algorithme Bully avec diffusion de la nouvelle adresse leader.
"""
import threading, time, logging, socket

log = logging.getLogger("Bully")

TIMEOUT_ELECTION  = 4.0
TIMEOUT_HEARTBEAT = 8.0
INTERVALLE_HB     = 2.0


class GestionnaireElection:

    def __init__(self, mon_id, mon_nom, mon_ip, mon_port_serveur,
                 callback_je_suis_leader, callback_nouveau_leader):
        self.mon_id             = mon_id
        self.mon_nom            = mon_nom
        self.mon_ip             = mon_ip
        self.mon_port_serveur   = mon_port_serveur
        self._cb_gagne          = callback_je_suis_leader
        self._cb_nouveau_leader = callback_nouveau_leader

        self._pairs             = {}   # pair_id → {"nom", "ip", "stub"}
        self._en_election       = False
        self._ok_recu           = False
        self._leader_id         = None
        self._dernier_hb        = time.time()
        self._lock              = threading.Lock()

    def ajouter_pair(self, pair_id, nom, ip, stub=None):
        with self._lock:
            self._pairs[pair_id] = {"nom": nom, "ip": ip, "stub": stub}

    def retirer_pair(self, pair_id):
        with self._lock:
            self._pairs.pop(pair_id, None)

    def set_stub_pair(self, pair_id, stub):
        with self._lock:
            if pair_id in self._pairs:
                self._pairs[pair_id]["stub"] = stub

    def signaler_hb_ok(self, leader_id):
        with self._lock:
            self._leader_id  = leader_id
            self._dernier_hb = time.time()

    def signaler_hb_timeout(self):
        with self._lock:
            if self._en_election:
                return
            self._en_election = True
        log.warning("[%s] Timeout heartbeat → élection Bully déclenchée", self.mon_nom)
        threading.Thread(target=self._lancer_election, daemon=True).start()

    def _lancer_election(self):
        with self._lock:
            self._ok_recu = False
            superieurs = {pid: info for pid, info in self._pairs.items()
                          if pid > self.mon_id}

        if not superieurs:
            log.info("[%s] Aucun pair supérieur → je suis le leader !", self.mon_nom)
            self._devenir_leader()
            return

        log.info("[%s] Envoi ELECTION → pairs supérieurs : %s",
                 self.mon_nom, [f"{pid}({info['nom']})" for pid, info in superieurs.items()])

        for pid, info in superieurs.items():
            self._envoyer_election(pid, info)

        time.sleep(TIMEOUT_ELECTION)

        with self._lock:
            ok = self._ok_recu
        if not ok:
            log.info("[%s] Aucun OK reçu → je suis le leader !", self.mon_nom)
            self._devenir_leader()
        else:
            log.info("[%s] OK reçu d'un pair supérieur → j'attends COORDINATEUR.", self.mon_nom)
            with self._lock:
                self._en_election = False

    def _envoyer_election(self, pair_id, info):
        stub = info.get("stub")
        if not stub:
            return
        try:
            from common import federated_pb2
            rep = stub.ElectionMessage(federated_pb2.ElectionRequest(
                candidate_id=f"ELECTION:{self.mon_id}:{self.mon_nom}",
                lamport_timestamp=0,
            ), timeout=3)
            if rep.accepted:
                with self._lock:
                    self._ok_recu = True
                log.info("[%s] OK reçu de %s (id=%d)", self.mon_nom, info['nom'], pair_id)
        except Exception:
            log.warning("[%s] Pair %s (id=%d) injoignable.", self.mon_nom, info['nom'], pair_id)

    def recevoir_election(self, candidat_id, candidat_nom):
        """Appelé quand on reçoit ELECTION d'un pair."""
        if self.mon_id > candidat_id:
            log.info("[%s] Reçu ELECTION de %s(id=%d) → je réponds OK et lance ma propre élection.",
                     self.mon_nom, candidat_nom, candidat_id)
            with self._lock:
                if not self._en_election:
                    self._en_election = True
                    threading.Thread(target=self._lancer_election, daemon=True).start()
            return True
        return False

    def recevoir_coordinateur(self, leader_id, leader_nom, leader_adresse):
        """Appelé quand on reçoit COORDINATEUR."""
        log.info("[%s] ══ COORDINATEUR reçu : %s(id=%d) @ %s ══",
                 self.mon_nom, leader_nom, leader_id, leader_adresse)
        with self._lock:
            self._leader_id   = leader_id
            self._en_election = False
        self._cb_nouveau_leader(leader_id, leader_nom, leader_adresse)

    def _devenir_leader(self):
        with self._lock:
            self._leader_id   = self.mon_id
            self._en_election = False

        adresse = f"{self.mon_ip}:{self.mon_port_serveur}"
        log.info("[%s] ══ ÉLUUU LEADER ! Adresse : %s ══", self.mon_nom, adresse)

        with self._lock:
            pairs = dict(self._pairs)

        for pid, info in pairs.items():
            self._envoyer_coordinateur(pid, info, adresse)

        self._cb_gagne()

    def _envoyer_coordinateur(self, pair_id, info, adresse):
        stub = info.get("stub")
        if not stub:
            return
        try:
            from common import federated_pb2
            stub.ElectionMessage(federated_pb2.ElectionRequest(
                candidate_id=f"COORD:{self.mon_id}:{self.mon_nom}:{adresse}",
                lamport_timestamp=0,
            ), timeout=3)
            log.info("[%s] COORDINATEUR envoyé à %s", self.mon_nom, info['nom'])
        except Exception:
            log.warning("[%s] Impossible d'envoyer COORDINATEUR à %s", self.mon_nom, info['nom'])

    @property
    def leader_id(self):
        with self._lock:
            return self._leader_id