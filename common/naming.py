"""
common/naming.py — Système de nommage à 3 niveaux.
Implémente les 3 types vus dans Van Steen & Tanenbaum chapitre 5 :
  - Nommage plat   : identifiant UUID opaque
  - Nommage structuré : arbre hiérarchique (type DNS)
  - Nommage par attributs : recherche par propriétés (type LDAP)
"""

import uuid


# ─────────────────────────────────────────────────────────────
# 1. NOMMAGE PLAT
#    Identifiant unique sans structure interne.
#    "Opaque" : son contenu ne révèle rien sur l'entité.
# ─────────────────────────────────────────────────────────────

class NommageFlat:
    """
    Table de correspondance : identifiant_plat → adresse réseau.
    Principe : on génère un UUID unique, on stocke l'adresse associée,
    on peut résoudre (retrouver l'adresse) à partir de l'ID.
    """

    def __init__(self):
        # Table : {client_id: {"ip": ..., "port": ...}}
        self._table = {}

    def enregistrer(self, ip: str, port: int) -> str:
        """
        Génère un identifiant plat unique et l'associe à une adresse.
        Retourne l'identifiant généré.
        """
        client_id = str(uuid.uuid4())[:8]
        self._table[client_id] = {"ip": ip, "port": port}
        return client_id

    def resoudre(self, client_id: str) -> dict | None:
        """
        Résolution : identifiant → adresse réseau.
        C'est l'opération fondamentale de tout système de nommage.
        Retourne None si l'ID est inconnu.
        """
        return self._table.get(client_id)

    def supprimer(self, client_id: str) -> bool:
        """Retire un client du registre (déconnexion)."""
        if client_id in self._table:
            del self._table[client_id]
            return True
        return False

    def lister(self) -> list:
        """Retourne tous les identifiants enregistrés."""
        return list(self._table.keys())


# ─────────────────────────────────────────────────────────────
# 2. NOMMAGE STRUCTURÉ
#    Arbre hiérarchique : chaque nœud a un label, le chemin
#    complet forme le nom (ex: /fl/yaounde/client-0d9b6e24).
#    Analogie directe avec le DNS et les systèmes de fichiers.
# ─────────────────────────────────────────────────────────────

class NoeudArbre:
    """Un nœud dans l'arbre de nommage."""
    def __init__(self, label: str):
        self.label = label
        self.enfants = {}       # label_enfant → NoeudArbre
        self.donnees = None     # données stockées (pour les feuilles)

    def __repr__(self):
        return f"Noeud({self.label})"


class NommageStructure:
    """
    Arbre de nommage hiérarchique.

    Structure pour notre projet FL :
        racine
        └── fl
            ├── yaounde
            │   ├── client-0d9b6e24   (feuille, stocke les infos client)
            │   └── client-9a4f3b1c
            ├── douala
            │   └── client-c326365b
            └── bafoussam
                └── client-95d5ca3d

    Le chemin /fl/yaounde/client-X est le "nom structuré" du client.
    """

    def __init__(self):
        self.racine = NoeudArbre("/")

    def _decomposer_chemin(self, chemin: str) -> list:
        """
        Décompose "/fl/yaounde/client-X" en ["fl", "yaounde", "client-X"].
        """
        return [p for p in chemin.strip("/").split("/") if p]

    def inserer(self, chemin: str, donnees: dict):
        """
        Insère une entrée dans l'arbre au chemin donné.
        Crée les nœuds intermédiaires si nécessaires.
        """
        labels = self._decomposer_chemin(chemin)
        noeud = self.racine
        for label in labels:
            if label not in noeud.enfants:
                noeud.enfants[label] = NoeudArbre(label)
            noeud = noeud.enfants[label]
        noeud.donnees = donnees

    def resoudre(self, chemin: str) -> dict | None:
        """
        Résolution : chemin structuré → données.
        Parcourt l'arbre label par label.
        """
        labels = self._decomposer_chemin(chemin)
        noeud = self.racine
        for label in labels:
            if label not in noeud.enfants:
                return None     # chemin inexistant
            noeud = noeud.enfants[label]
        return noeud.donnees

    def lister_sous_arbre(self, chemin: str) -> list:
        """
        Liste toutes les feuilles sous un chemin donné.
        Ex: lister_sous_arbre("/fl/yaounde") → tous les clients de Yaoundé.
        C'est l'avantage du nommage structuré : naviguer par zone.
        """
        labels = self._decomposer_chemin(chemin)
        noeud = self.racine
        for label in labels:
            if label not in noeud.enfants:
                return []
            noeud = noeud.enfants[label]

        # Collecter toutes les feuilles (nœuds avec données)
        resultats = []
        self._collecter_feuilles(noeud, chemin, resultats)
        return resultats

    def _collecter_feuilles(self, noeud: NoeudArbre, chemin: str, resultats: list):
        """Parcours récursif en profondeur pour collecter les feuilles."""
        if noeud.donnees is not None:
            resultats.append({"chemin": chemin, "donnees": noeud.donnees})
        for label, enfant in noeud.enfants.items():
            self._collecter_feuilles(enfant, f"{chemin}/{label}", resultats)

    def afficher_arbre(self, noeud: NoeudArbre = None, indent: int = 0):
        """Affiche l'arbre de façon lisible (utile pour la démo)."""
        if noeud is None:
            noeud = self.racine
        prefixe = "  " * indent + ("└─ " if indent > 0 else "")
        info = f" [{noeud.donnees['name']}]" if noeud.donnees else ""
        print(f"{prefixe}{noeud.label}{info}")
        for enfant in noeud.enfants.values():
            self.afficher_arbre(enfant, indent + 1)


# ─────────────────────────────────────────────────────────────
# 3. NOMMAGE PAR ATTRIBUTS
#    On ne cherche pas par ID ni par chemin.
#    On décrit ce qu'on veut, l'annuaire retourne les correspondances.
#    Principe des annuaires LDAP, bases NoSQL.
# ─────────────────────────────────────────────────────────────

class NommageAttributs:
    """
    Annuaire de nommage par attributs.
    Chaque entrée est un ensemble de paires clé-valeur.
    La recherche retourne toutes les entrées qui satisfont un filtre.

    Opérateurs supportés dans les valeurs de filtre :
      ">X"  : attribut numérique supérieur à X
      ">=X" : supérieur ou égal
      "<X"  : inférieur
      "valeur" : égalité exacte (chaîne)
    """

    def __init__(self):
        # Liste de tuples (client_id, attributs_dict)
        self._entrees = []

    def enregistrer(self, client_id: str, attributs: dict):
        """
        Enregistre un client avec ses attributs.
        attributs = {"region": "yaounde", "cpu_threads": "4", "ram_fraction": "0.8", ...}
        """
        # Éviter les doublons
        self._entrees = [(cid, attrs) for cid, attrs in self._entrees if cid != client_id]
        self._entrees.append((client_id, dict(attributs)))

    def chercher(self, filtre: dict) -> list:
        """
        Recherche par attributs.
        Retourne la liste des client_id dont les attributs satisfont TOUS les critères.

        Exemple :
          chercher({"region": "yaounde"})
          chercher({"region": "yaounde", "cpu_threads": ">2"})
          chercher({"ram_fraction": ">=0.5"})
        """
        resultats = []
        for client_id, attributs in self._entrees:
            if self._satisfait(attributs, filtre):
                resultats.append({"client_id": client_id, "attributs": attributs})
        return resultats

    def _satisfait(self, attributs: dict, filtre: dict) -> bool:
        """Vérifie qu'un enregistrement satisfait tous les critères du filtre."""
        for cle, critere in filtre.items():
            if cle not in attributs:
                return False
            valeur = attributs[cle]

            # Opérateurs numériques
            if isinstance(critere, str) and critere.startswith(">="):
                try:
                    if float(valeur) < float(critere[2:]):
                        return False
                except ValueError:
                    return False
            elif isinstance(critere, str) and critere.startswith(">"):
                try:
                    if float(valeur) <= float(critere[1:]):
                        return False
                except ValueError:
                    return False
            elif isinstance(critere, str) and critere.startswith("<="):
                try:
                    if float(valeur) > float(critere[2:]):
                        return False
                except ValueError:
                    return False
            elif isinstance(critere, str) and critere.startswith("<"):
                try:
                    if float(valeur) >= float(critere[1:]):
                        return False
                except ValueError:
                    return False
            else:
                # Égalité exacte
                if str(valeur) != str(critere):
                    return False
        return True

    def supprimer(self, client_id: str):
        """Retire un client de l'annuaire."""
        self._entrees = [(cid, a) for cid, a in self._entrees if cid != client_id]

    def lister_tout(self) -> list:
        return list(self._entrees)


# ─────────────────────────────────────────────────────────────
# 4. ANNUAIRE UNIFIÉ
#    Combine les 3 systèmes. C'est lui qu'utilise le serveur.
# ─────────────────────────────────────────────────────────────

class AnnuaireUnifie:
    """
    Point d'entrée unique pour les 3 systèmes de nommage.
    Lors d'un RegisterClient, les 3 systèmes sont mis à jour simultanément.
    """

    def __init__(self):
        self.plat       = NommageFlat()
        self.structure  = NommageStructure()
        self.attributs  = NommageAttributs()

        # Stockage complet des infos client (référence principale)
        self._clients = {}

    def enregistrer_client(self, nom: str, ip: str, port: int,
                            region: str, dataset_size: int,
                            attrs_supplementaires: dict = None) -> dict:
        """
        Enregistre un client dans les 3 systèmes simultanément.
        Retourne un dict avec les 3 identifiants attribués.
        """
        # 1. Nommage PLAT : générer l'UUID
        client_id = self.plat.enregistrer(ip, port)

        # 2. Nommage STRUCTURÉ : chemin hiérarchique
        chemin = f"/fl/{region}/client-{client_id}"
        donnees_client = {
            "name": nom, "ip": ip, "port": port,
            "region": region, "dataset_size": dataset_size,
            "client_id": client_id,
        }
        self.structure.inserer(chemin, donnees_client)

        # 3. Nommage par ATTRIBUTS : enregistrer avec toutes les propriétés
        attributs = {
            "name": nom,
            "region": region,
            "dataset_size": str(dataset_size),
        }
        if attrs_supplementaires:
            attributs.update(attrs_supplementaires)
        self.attributs.enregistrer(client_id, attributs)

        # Référence principale
        self._clients[client_id] = {**donnees_client, "chemin": chemin, "attributs": attributs}

        return {
            "client_id": client_id,          # nommage plat
            "chemin": chemin,                 # nommage structuré
            "attributs": attributs,           # nommage par attributs
        }

    def deconnecter_client(self, client_id: str):
        """Retire un client des 3 systèmes."""
        chemin = self._clients.get(client_id, {}).get("chemin")
        self.plat.supprimer(client_id)
        if chemin:
            self.structure.inserer(chemin, None)   # vider la feuille
        self.attributs.supprimer(client_id)
        self._clients.pop(client_id, None)

    def get_client(self, client_id: str) -> dict | None:
        return self._clients.get(client_id)

    def clients_par_region(self, region: str) -> list:
        """Nommage structuré : tous les clients d'une région."""
        return self.structure.lister_sous_arbre(f"/fl/{region}")

    def clients_par_criteres(self, filtre: dict) -> list:
        """Nommage par attributs : recherche flexible."""
        return self.attributs.chercher(filtre)

    def afficher_etat(self):
        """Affiche l'état complet de l'annuaire (utile pour la démo)."""
        print("\n══ ANNUAIRE — Nommage structuré ══")
        self.structure.afficher_arbre()
        print(f"\n══ {len(self._clients)} clients enregistrés ══")
        for cid, info in self._clients.items():
            print(f"  [{cid}] {info['name']} | {info['chemin']} | dataset={info['dataset_size']}")
