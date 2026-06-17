"""
common/lamport_clock.py — Horloge logique de Lamport.
Van Steen & Tanenbaum, chapitre 6.

Les 3 règles :
  R1 — Événement interne  : LC = LC + 1
  R2 — Envoi de message   : LC = LC + 1  ;  message.timestamp = LC
  R3 — Réception          : LC = max(LC, timestamp_recu) + 1
"""

import threading


class HorlogeLamport:
    """
    Horloge logique de Lamport thread-safe.
    Chaque processus (serveur ou client) possède une instance.
    """

    def __init__(self, nom: str = ""):
        self._lc = 0                      # compteur logique
        self._lock = threading.Lock()     # thread-safe (plusieurs clients en parallèle)
        self.nom = nom                    # pour les logs

    # ── Règle 1 : événement interne ──────────────────────────
    def evenement_local(self) -> int:
        """
        Appeler avant tout événement interne (calcul, décision...).
        Retourne le nouveau timestamp.
        """
        with self._lock:
            self._lc += 1
            return self._lc

    # ── Règle 2 : envoi de message ───────────────────────────
    def avant_envoi(self) -> int:
        """
        Appeler juste avant d'envoyer un message.
        Retourne le timestamp à placer dans le message.
        """
        with self._lock:
            self._lc += 1
            return self._lc

    # ── Règle 3 : réception de message ───────────────────────
    def apres_reception(self, timestamp_recu: int) -> int:
        """
        Appeler dès qu'un message est reçu.
        Synchronise l'horloge locale avec le timestamp du message.
        Retourne le nouveau timestamp local.
        """
        with self._lock:
            self._lc = max(self._lc, timestamp_recu) + 1
            return self._lc

    @property
    def valeur(self) -> int:
        """Lire la valeur courante sans modifier l'horloge."""
        with self._lock:
            return self._lc

    def __repr__(self):
        return f"HorlogeLamport({self.nom}, LC={self._lc})"


class JournalEvenements:
    """
    Enregistre tous les événements horodatés.
    Utile pour la démo : on peut trier les événements par timestamp
    et montrer que l'ordre causal est respecté même entre machines.
    """

    def __init__(self):
        self._journal = []
        self._lock = threading.Lock()

    def enregistrer(self, timestamp: int, source: str, type_evt: str, details: str = ""):
        with self._lock:
            self._journal.append({
                "timestamp": timestamp,
                "source":    source,
                "type":      type_evt,
                "details":   details,
            })

    def afficher_ordre_causal(self):
        """
        Trie et affiche les événements par timestamp de Lamport.
        C'est ici qu'on voit la cohérence causale en action.
        """
        with self._lock:
            tries = sorted(self._journal, key=lambda e: e["timestamp"])

        print("\n══ Journal des événements (ordre causal de Lamport) ══")
        for evt in tries:
            print(f"  [LC={evt['timestamp']:3d}] {evt['source']:15s} | {evt['type']:20s} | {evt['details']}")

    def get_journal(self) -> list:
        with self._lock:
            return list(self._journal)