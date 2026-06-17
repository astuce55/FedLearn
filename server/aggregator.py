"""
server/aggregator.py — Agrégation FedAvg.
Van Steen & Tanenbaum + McMahan et al. 2017 (papier original FedAvg).

Formule : W_global = sum_k( (n_k / N) * W_k )
  W_k  = poids du client k après entraînement local
  n_k  = taille du dataset du client k
  N    = sum(n_k) = total des exemples
"""

import logging
import threading

log = logging.getLogger(__name__)


class AggregateurFedAvg:
    """
    Collecte les mises à jour locales des clients et agrège avec FedAvg.

    Cycle d'un round :
      1. demarrer_round()   → réinitialise la collecte
      2. ajouter_update()   → chaque client envoie ses poids
      3. agreger()          → calcule W_global quand tous ont répondu
    """

    def __init__(self, nb_clients_attendus: int, taille_modele: int = 2):
        self.nb_clients_attendus = nb_clients_attendus
        self.taille_modele = taille_modele

        # Poids globaux courants (initialisés à zéro)
        self.poids_globaux = [0.0] * taille_modele

        # Collecte du round en cours
        self._updates = []          # liste de {"client_id", "poids", "n_k"}
        self._lock = threading.Lock()
        self._round_actuel = 0
        self._historique = []       # pour tracer la convergence

    def demarrer_round(self):
        """Réinitialise la collecte pour un nouveau round."""
        with self._lock:
            self._round_actuel += 1
            self._updates = []
            log.info("Round %d démarré. Attente de %d clients.",
                     self._round_actuel, self.nb_clients_attendus)

    def ajouter_update(self, client_id: str, poids: list, n_k: int,
                       lamport_ts: int) -> bool:
        """
        Enregistre la mise à jour d'un client.
        Retourne True si tous les clients ont répondu (round complet).
        """
        with self._lock:
            self._updates.append({
                "client_id":   client_id,
                "poids":       list(poids),
                "n_k":         n_k,
                "lamport_ts":  lamport_ts,
            })
            nb_recus = len(self._updates)
            log.info("Update reçue (%d/%d) — client=%s n_k=%d LC=%d",
                     nb_recus, self.nb_clients_attendus, client_id[:6], n_k, lamport_ts)
            return nb_recus >= self.nb_clients_attendus

    def agreger(self) -> list:
        """
        Applique FedAvg sur les updates collectées.

        W_global = sum_k( (n_k / N) * W_k )

        Les updates sont triées par lamport_timestamp avant agrégation
        pour respecter l'ordre causal (lien avec étape 3).
        """
        with self._lock:
            if not self._updates:
                return self.poids_globaux

            # Tri causal : ordre de Lamport avant d'agréger
            updates_tries = sorted(self._updates, key=lambda u: u["lamport_ts"])

            # Total des exemples
            N = sum(u["n_k"] for u in updates_tries)
            if N == 0:
                return self.poids_globaux

            # Moyenne pondérée
            nouveaux_poids = [0.0] * self.taille_modele
            for u in updates_tries:
                poids_client = float(u["n_k"]) / N
                for i, w in enumerate(u["poids"]):
                    nouveaux_poids[i] += poids_client * w

            self.poids_globaux = nouveaux_poids

            # Sauvegarder pour la courbe de convergence
            self._historique.append({
                "round":  self._round_actuel,
                "poids":  list(nouveaux_poids),
                "N":      N,
                "clients": len(updates_tries),
            })

            log.info(
                "FedAvg round %d — N=%d clients=%d → W=%s",
                self._round_actuel, N, len(updates_tries),
                [round(w, 4) for w in nouveaux_poids]
            )
            return list(nouveaux_poids)

    def get_historique(self) -> list:
        """Retourne l'historique des rounds pour le benchmarking."""
        with self._lock:
            return list(self._historique)

    def tous_recus(self) -> bool:
        with self._lock:
            return len(self._updates) >= self.nb_clients_attendus