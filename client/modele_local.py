"""
client/modele_local.py — Modèle local et entraînement.

On simule une régression linéaire : y = w*x + b
Paramètres du modèle = [w, b]

Chaque client a des données légèrement différentes (bruit aléatoire)
pour simuler l'hétérogénéité des données dans FL.
La vraie solution globale est w=2.0, b=1.0 (y = 2x + 1).
"""

import random
import math


class ModeleLineaire:
    """
    Régression linéaire : y = w*x + b
    Entraînée par descente de gradient (SGD).
    """

    def __init__(self, w_init: float = 0.0, b_init: float = 0.0):
        self.w = w_init
        self.b = b_init

    def predire(self, x: float) -> float:
        return self.w * x + self.b

    def erreur(self, x: float, y: float) -> float:
        """Erreur quadratique : (y_pred - y_reel)²"""
        return (self.predire(x) - y) ** 2

    def get_poids(self) -> list:
        return [self.w, self.b]

    def set_poids(self, poids: list):
        self.w, self.b = poids[0], poids[1]


def generer_donnees_locales(n: int, bruit: float = 0.3, seed: int = 42) -> list:
    """
    Génère n exemples (x, y) autour de la vraie fonction y = 2x + 1.
    Le bruit simule l'hétérogénéité des données entre clients.
    Chaque client a un seed différent → données différentes.
    """
    random.seed(seed)
    donnees = []
    for _ in range(n):
        x = random.uniform(-1, 1)
        y = 2.0 * x + 1.0 + random.gauss(0, bruit)
        donnees.append((x, y))
    return donnees


def entrainer_local(poids_globaux: list, donnees: list,
                    lr: float = 0.01, nb_epochs: int = 5) -> tuple:
    """
    Entraîne le modèle local par SGD à partir des poids globaux reçus.

    Paramètres :
      poids_globaux : [w, b] reçus du serveur
      donnees       : liste de (x, y) locaux
      lr            : taux d'apprentissage (learning rate)
      nb_epochs     : nombre de passes sur les données

    Retourne (nouveaux_poids, perte_finale).
    """
    modele = ModeleLineaire(w_init=poids_globaux[0], b_init=poids_globaux[1])
    n = len(donnees)

    for epoch in range(nb_epochs):
        perte_totale = 0.0
        # Mélanger les données à chaque epoch (SGD)
        random.shuffle(donnees)

        for x, y in donnees:
            y_pred = modele.predire(x)
            erreur  = y_pred - y

            # Gradient de la loss MSE par rapport à w et b
            grad_w = 2 * erreur * x
            grad_b = 2 * erreur

            # Mise à jour SGD
            modele.w -= lr * grad_w
            modele.b -= lr * grad_b

            perte_totale += erreur ** 2

        perte_moy = perte_totale / n

    return modele.get_poids(), perte_moy