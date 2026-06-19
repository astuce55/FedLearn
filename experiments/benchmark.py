"""
experiments/benchmark.py — Mesures de performance pour le rapport INF4218.

Mesure 1 : Latence des RPCs (RegisterClient, GetGlobalModel, SendLocalUpdate)
Mesure 2 : Convergence FedAvg sur N rounds avec clients hétérogènes
Mesure 3 : Démonstration de l'ordre causal des horloges de Lamport
Mesure 4 : Impact du nombre de clients sur le temps d'agrégation

Génère 4 fichiers CSV dans experiments/results/
"""
import grpc, time, sys, os, csv, threading, random, statistics
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from concurrent import futures

from common import federated_pb2, federated_pb2_grpc
from common.checkpoint import supprimer
from server.server import FederatedServer
from client.client import FederatedClient
from client.modele_local import generer_donnees_locales, entrainer_local

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(RESULTS_DIR, exist_ok=True)

def sauver_csv(nom_fichier, entetes, lignes):
    chemin = os.path.join(RESULTS_DIR, nom_fichier)
    with open(chemin, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(entetes)
        w.writerows(lignes)
    print(f"  → Sauvegardé : {chemin}")
    return chemin


def demarrer_serveur_test(port, nb_clients):
    supprimer()
    srv_inst = FederatedServer(nb_clients_par_round=nb_clients, reprendre=False)
    srv = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    federated_pb2_grpc.add_FederatedLearningServiceServicer_to_server(srv_inst, srv)
    srv.add_insecure_port(f"0.0.0.0:{port}")
    srv.start()
    time.sleep(0.3)
    return srv, srv_inst


# ═══════════════════════════════════════════════════════════════
# MESURE 1 : Latence des RPCs
# ═══════════════════════════════════════════════════════════════

def mesure_latence_rpcs(nb_repetitions=50):
    print("\n[Mesure 1] Latence des RPCs...")
    srv, srv_inst = demarrer_serveur_test(50061, 1)

    canal = grpc.insecure_channel("localhost:50061")
    stub  = federated_pb2_grpc.FederatedLearningServiceStub(canal)

    resultats = []

    for i in range(nb_repetitions):
        # RegisterClient
        t0 = time.perf_counter()
        rep = stub.RegisterClient(federated_pb2.RegisterRequest(
            client_name=f"bench-{i}", ip_address="127.0.0.1",
            port=0, dataset_size=100,
            attributes={"region": "yaounde"},
            lamport_timestamp=i,
        ))
        latence_register = (time.perf_counter() - t0) * 1000

        client_id = rep.assigned_id

        # GetGlobalModel
        t0 = time.perf_counter()
        stub.GetGlobalModel(federated_pb2.ModelRequest(
            client_id=client_id, lamport_timestamp=i))
        latence_get = (time.perf_counter() - t0) * 1000

        # GetStatus
        t0 = time.perf_counter()
        stub.GetStatus(federated_pb2.StatusRequest())
        latence_status = (time.perf_counter() - t0) * 1000

        # Heartbeat
        t0 = time.perf_counter()
        stub.Heartbeat(federated_pb2.HeartbeatRequest(
            sender_id=client_id, lamport_timestamp=i))
        latence_hb = (time.perf_counter() - t0) * 1000

        resultats.append([i, round(latence_register,3), round(latence_get,3),
                          round(latence_status,3), round(latence_hb,3)])

    canal.close()
    srv.stop(0)

    # Statistiques
    for col_idx, nom in enumerate(["RegisterClient","GetGlobalModel","GetStatus","Heartbeat"], 1):
        vals = [r[col_idx] for r in resultats]
        print(f"  {nom:20s} | moy={statistics.mean(vals):.2f}ms "
              f"min={min(vals):.2f}ms max={max(vals):.2f}ms "
              f"std={statistics.stdev(vals):.2f}ms")

    sauver_csv("latence_rpcs.csv",
               ["iteration","RegisterClient_ms","GetGlobalModel_ms",
                "GetStatus_ms","Heartbeat_ms"],
               resultats)
    return resultats


# ═══════════════════════════════════════════════════════════════
# MESURE 2 : Convergence FedAvg
# ═══════════════════════════════════════════════════════════════

def mesure_convergence(nb_rounds=15, nb_clients=3):
    print(f"\n[Mesure 2] Convergence FedAvg ({nb_rounds} rounds, {nb_clients} clients)...")
    srv, srv_inst = demarrer_serveur_test(50062, nb_clients)

    # Configs hétérogènes : dataset et vitesse différents
    configs = [
        ("C1", 300, 42,  4, 0.8),   # puissant, petit dataset
        ("C2", 500, 99,  2, 0.5),   # moyen
        ("C3", 150, 7,   1, 0.2),   # faible, petit dataset
    ][:nb_clients]

    clients = []
    donnees_list = []
    for nom, ds, seed, cpu, ram in configs:
        c = FederatedClient("localhost:50062", nom, ds, "yaounde", cpu, ram,
                            mon_id=random.randint(1,999))
        c.rejoindre_reseau()
        clients.append(c)
        donnees_list.append(generer_donnees_locales(ds, seed=seed))

    resultats = []

    for round_num in range(nb_rounds):
        t_debut = time.perf_counter()

        # Lancer le round
        srv_inst.lancer_round(nb_clients)

        # Tous les clients s'entraînent en parallèle
        def faire_round(client, donnees, rn):
            for _ in range(30):
                status = client._get_status()
                if status and status.round_en_cours and status.round_actuel == rn:
                    poids, _ = client.get_modele_global()
                    nouveaux, perte = entrainer_local(poids, donnees)
                    rep = client.envoyer_mise_a_jour(nouveaux, rn)
                    if rep and rep.received:
                        return perte
                time.sleep(0.05)
            return None

        threads = []
        pertes  = [None] * nb_clients
        for i, (c, d) in enumerate(zip(clients, donnees_list)):
            def run(idx=i, client=c, data=d, rn=round_num):
                pertes[idx] = faire_round(client, data, rn)
            t = threading.Thread(target=run)
            threads.append(t)

        for t in threads: t.start()
        for t in threads: t.join()
        time.sleep(0.1)

        duree = (time.perf_counter() - t_debut) * 1000
        poids = srv_inst.aggregateur.poids_globaux
        perte_moy = statistics.mean([p for p in pertes if p is not None] or [0])

        erreur_w = abs(poids[0] - 2.0)
        erreur_b = abs(poids[1] - 1.0)
        erreur_tot = erreur_w + erreur_b

        resultats.append([
            round_num, round(poids[0],5), round(poids[1],5),
            round(erreur_tot,5), round(perte_moy,5), round(duree,1)
        ])
        print(f"  Round {round_num:2d} | w={poids[0]:.4f} b={poids[1]:.4f} "
              f"| erreur={erreur_tot:.4f} | perte={perte_moy:.4f} | {duree:.0f}ms")

    srv.stop(0)
    sauver_csv("convergence_fedavg.csv",
               ["round","w","b","erreur_totale","perte_moyenne","duree_ms"],
               resultats)
    return resultats


# ═══════════════════════════════════════════════════════════════
# MESURE 3 : Horloges de Lamport — ordre causal
# ═══════════════════════════════════════════════════════════════

def mesure_lamport(nb_clients=3, nb_echanges=10):
    print(f"\n[Mesure 3] Horloges de Lamport ({nb_clients} clients, {nb_echanges} échanges)...")
    srv, srv_inst = demarrer_serveur_test(50063, nb_clients)

    clients_obj = []
    for i in range(nb_clients):
        c = FederatedClient("localhost:50063", f"C{i+1}", 100, "yaounde",
                            mon_id=i+1)
        c.rejoindre_reseau()
        clients_obj.append(c)

    resultats = []
    evt_id = 0

    for echange in range(nb_echanges):
        srv_inst.lancer_round(nb_clients)

        for c in clients_obj:
            lc_avant = c.horloge.valeur
            poids, _ = c.get_modele_global()
            lc_apres = c.horloge.valeur
            resultats.append([evt_id, c.nom, "GetGlobalModel",
                               lc_avant, lc_apres, lc_apres > lc_avant])
            evt_id += 1

        for i, c in enumerate(clients_obj):
            lc_avant = c.horloge.valeur
            nouveaux, _ = entrainer_local(poids, generer_donnees_locales(100, seed=i))
            rep = c.envoyer_mise_a_jour(nouveaux, echange)
            lc_apres = c.horloge.valeur
            resultats.append([evt_id, c.nom, "SendLocalUpdate",
                               lc_avant, lc_apres, lc_apres > lc_avant])
            evt_id += 1

        time.sleep(0.1)

    srv.stop(0)

    violations = [r for r in resultats if not r[5]]
    print(f"  {len(resultats)} événements | violations ordre causal : {len(violations)}")
    print(f"  → LC croissant dans 100% des cas : {len(violations)==0}")

    sauver_csv("lamport_ordre_causal.csv",
               ["evt_id","client","rpc","lc_avant","lc_apres","ordre_respecte"],
               resultats)
    return resultats


# ═══════════════════════════════════════════════════════════════
# MESURE 4 : Impact du nombre de clients sur le temps de round
# ═══════════════════════════════════════════════════════════════

def mesure_scalabilite(configs_clients=[1, 2, 3, 5]):
    print(f"\n[Mesure 4] Scalabilité ({configs_clients} clients)...")
    resultats = []

    for nb in configs_clients:
        srv, srv_inst = demarrer_serveur_test(50064, nb)
        clients_loc   = []
        donnees_loc   = []

        for i in range(nb):
            c = FederatedClient("localhost:50064", f"S{i+1}", 200, "yaounde",
                                mon_id=i+1)
            c.rejoindre_reseau()
            clients_loc.append(c)
            donnees_loc.append(generer_donnees_locales(200, seed=i*7))

        # Mesurer 5 rounds
        durees = []
        for rn in range(5):
            t0 = time.perf_counter()
            srv_inst.lancer_round(nb)

            def faire(client=None, data=None, r=rn):
                for _ in range(40):
                    s = client._get_status()
                    if s and s.round_en_cours and s.round_actuel == r:
                        p, _ = client.get_modele_global()
                        np2, _ = entrainer_local(p, data)
                        rep = client.envoyer_mise_a_jour(np2, r)
                        if rep and rep.received: return
                    time.sleep(0.05)

            ts = [threading.Thread(target=faire, kwargs={"client":c,"data":d})
                  for c,d in zip(clients_loc,donnees_loc)]
            for t in ts: t.start()
            for t in ts: t.join()
            time.sleep(0.1)
            durees.append((time.perf_counter()-t0)*1000)

        moy = statistics.mean(durees)
        std = statistics.stdev(durees) if len(durees)>1 else 0
        print(f"  {nb} client(s) : moy={moy:.0f}ms std={std:.0f}ms")
        resultats.append([nb, round(moy,1), round(std,1),
                          round(min(durees),1), round(max(durees),1)])
        srv.stop(0)
        time.sleep(0.5)

    sauver_csv("scalabilite.csv",
               ["nb_clients","duree_moy_ms","duree_std_ms","min_ms","max_ms"],
               resultats)
    return resultats


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("="*60)
    print("  Benchmark INF4218 — Système FL Distribué")
    print("="*60)

    r1 = mesure_latence_rpcs(nb_repetitions=30)
    r2 = mesure_convergence(nb_rounds=10, nb_clients=3)
    r3 = mesure_lamport(nb_clients=3, nb_echanges=5)
    r4 = mesure_scalabilite([1, 2, 3])

    print("\n" + "="*60)
    print("  Benchmark terminé. Fichiers dans experiments/results/")
    print("="*60)