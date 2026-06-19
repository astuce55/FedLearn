# Système d'Apprentissage Fédéré Distribué — INF4218

Projet de programmation distribuée implémentant un système d'apprentissage fédéré
avec les concepts fondamentaux de Van Steen & Tanenbaum (chapitres 1 à 8) :
nommage plat/structuré/attributs, horloges de Lamport, élection Bully, FedAvg,
checkpoint et tolérance aux pannes.

---

## Structure du projet

```
fl_distributed_system/
├── proto/
│   └── federated.proto          # Contrat IDL gRPC
├── common/
│   ├── federated_pb2.py         # Généré automatiquement
│   ├── federated_pb2_grpc.py    # Généré automatiquement
│   ├── naming.py                # 3 systèmes de nommage
│   ├── lamport_clock.py         # Horloges de Lamport
│   ├── bully.py                 # Algorithme d'élection Bully
│   └── checkpoint.py            # Sauvegarde / reprise d'état
├── server/
│   ├── server.py                # Serveur central + menu de contrôle
│   └── aggregator.py            # Agrégation FedAvg
├── client/
│   ├── client.py                # Client FL + heartbeat + Bully
│   └── modele_local.py          # Régression linéaire + SGD
├── experiments/
│   ├── benchmark.py             # Mesures de performance
│   ├── plot_results.py          # Génération des figures
│   └── results/                 # CSV générés par le benchmark
│       └── figures/             # Figures SVG
├── requirements.txt
└── README.md
```

---

## Installation

Sur **chaque machine** (serveur et clients) :

```bash
# 1. Cloner le dépôt
git clone <url-du-depot> fl_distributed_system
cd fl_distributed_system

# 2. Installer les dépendances
pip install -r requirements.txt

# 3. Générer les stubs gRPC (une seule fois)
python -m grpc_tools.protoc \
  -I proto/ \
  --python_out=common/ \
  --grpc_python_out=common/ \
  proto/federated.proto

# 4. Corriger l'import dans le fichier généré
sed -i 's/^import federated_pb2/from . import federated_pb2/' \
  common/federated_pb2_grpc.py
```

---

## Lancement

### 1. Sur la machine serveur

```bash
python -m server.server
```

Le serveur affiche automatiquement son adresse IP à communiquer aux clients :

```
╔══════════════════════════════════════════════════════╗
║  Serveur FL démarré !                                ║
║  Adresse → 192.168.1.10:50051                        ║
╚══════════════════════════════════════════════════════╝
```

Ouvrir le port si nécessaire :

```bash
sudo ufw allow 50051
sudo ufw allow 50052   # port du leader de secours (élection Bully)
```

### 2. Sur chaque machine cliente

```bash
python -m client.client
```

Le menu demande :
- Adresse du serveur (ex : `192.168.1.10:50051`)
- Prénom / pseudo
- Région
- Taille du dataset local
- Ressources CPU et RAM à allouer

Le client s'enregistre puis attend qu'un round soit lancé depuis le serveur.

### 3. Menu de contrôle du serveur

Une fois les clients connectés, le menu serveur permet :

| Commande | Action |
|---|---|
| `1` | Voir les clients connectés (IP, région, dataset, ressources) |
| `2` | Lancer un round d'entraînement (choisir le nombre de clients) |
| `3` | Historique des rounds et convergence |
| `4` | Supprimer un client du registre |
| `5` | État du dernier checkpoint |
| `0` | Arrêter le serveur |

---

## Simulation d'une panne et élection Bully

1. Lancer au moins un round (`2` dans le menu) pour créer un checkpoint
2. Arrêter le serveur avec **Ctrl+C**
3. Après 8 secondes, les clients détectent la panne via le heartbeat timeout
4. L'algorithme Bully s'exécute automatiquement : le client avec le plus grand
   `bully_id` remporte l'élection
5. Le nouveau leader démarre un serveur de secours sur le port **50052**,
   charge le checkpoint et affiche son propre menu de contrôle
6. Les autres clients se reconnectent automatiquement au nouveau leader

---

## Benchmark et figures

```bash
# Lancer toutes les mesures (génère les CSV dans experiments/results/)
python -m experiments.benchmark

# Générer les figures SVG depuis les CSV
python -m experiments.plot_results
```

Les figures sont enregistrées dans `experiments/figures/` et s'ouvrent
directement dans un navigateur web.

---

## Concepts implémentés

| Concept | Fichier | Chapitre Van Steen |
|---|---|---|
| Nommage plat (UUID) | `common/naming.py` | Ch. 5 |
| Nommage structuré (arbre) | `common/naming.py` | Ch. 5 |
| Nommage par attributs (LDAP-like) | `common/naming.py` | Ch. 5 |
| Horloges logiques de Lamport | `common/lamport_clock.py` | Ch. 6 |
| Élection de leader (Bully) | `common/bully.py` | Ch. 7 |
| Communication RPC/gRPC | `proto/federated.proto` | Ch. 4 |
| Agrégation FedAvg | `server/aggregator.py` | Ch. 2 |
| Checkpoint et reprise | `common/checkpoint.py` | Ch. 8 |
| Heartbeat et tolérance aux pannes | `client/client.py` | Ch. 8 |

---

## Auteurs

Projet réalisé dans le cadre du cours **INF4218 — Programmation Distribuée**  
Master 1 Informatique — Systèmes et Réseaux  
Université de Yaoundé I
