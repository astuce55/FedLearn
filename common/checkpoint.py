"""
common/checkpoint.py — Sauvegarde et reprise de l'état du système.
Van Steen & Tanenbaum, chapitre 8 — Tolérance aux pannes.

Stratégie : checkpoint après chaque round FL réussi.
En cas de panne → le nouveau leader charge le dernier checkpoint
et reprend au round suivant. Aucune perte sur les rounds terminés.
"""

import json
import os
import time
import logging

log = logging.getLogger(__name__)

CHECKPOINT_DIR  = "/tmp/fl_checkpoints"
CHECKPOINT_FILE = os.path.join(CHECKPOINT_DIR, "checkpoint_latest.json")


def sauvegarder(round_num: int, poids_globaux: list,
                clients: dict, lamport_clock: int,
                leader_id: int = None):
    """
    Sauvegarde l'état complet du système après un round réussi.

    Appelé automatiquement par le serveur après chaque FedAvg.
    C'est le point de reprise en cas de panne.
    """
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)

    etat = {
        "round":          round_num,
        "poids_globaux":  poids_globaux,
        "clients":        clients,          # {client_id: {"name", "region", ...}}
        "lamport_clock":  lamport_clock,
        "leader_id":      leader_id,
        "timestamp_reel": time.time(),      # horloge physique (pour info seulement)
        "timestamp_lisible": time.strftime("%Y-%m-%d %H:%M:%S"),
    }

    # Écriture atomique : on écrit dans un fichier temporaire
    # puis on renomme → évite un checkpoint corrompu si la machine
    # plante pendant l'écriture (technique standard)
    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(etat, f, indent=2)
    os.replace(tmp, CHECKPOINT_FILE)

    log.info("Checkpoint sauvegardé | round=%d | poids=%s | %d clients",
             round_num, [round(p, 4) for p in poids_globaux], len(clients))
    return CHECKPOINT_FILE


def charger() -> dict | None:
    """
    Charge le dernier checkpoint disponible.
    Retourne None si aucun checkpoint n'existe (premier démarrage).
    """
    if not os.path.exists(CHECKPOINT_FILE):
        log.info("Aucun checkpoint trouvé. Démarrage à zéro.")
        return None

    with open(CHECKPOINT_FILE, "r") as f:
        etat = json.load(f)

    log.info("Checkpoint chargé | round=%d | poids=%s | sauvegardé le %s",
             etat["round"],
             [round(p, 4) for p in etat["poids_globaux"]],
             etat.get("timestamp_lisible", "?"))
    return etat


def checkpoint_existe() -> bool:
    return os.path.exists(CHECKPOINT_FILE)


def supprimer():
    """Supprime le checkpoint (utile pour les tests)."""
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        log.info("Checkpoint supprimé.")