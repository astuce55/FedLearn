"""
experiments/plot_results.py — Génère les figures du rapport depuis les CSV.

Usage :
  python -m experiments.plot_results

Lit les fichiers dans experiments/results/
Génère les figures dans experiments/figures/
"""
import csv, os, sys, math

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
FIGURES_DIR = os.path.join(os.path.dirname(__file__), "figures")
os.makedirs(FIGURES_DIR, exist_ok=True)


def lire_csv(nom):
    chemin = os.path.join(RESULTS_DIR, nom)
    if not os.path.exists(chemin):
        print(f"  ⚠ Fichier manquant : {chemin}")
        print(f"    Lance d'abord : python -m experiments.benchmark")
        return [], []
    with open(chemin) as f:
        reader = csv.DictReader(f)
        lignes = list(reader)
    return lignes, list(lignes[0].keys()) if lignes else []


def sauver_svg(nom, contenu):
    chemin = os.path.join(FIGURES_DIR, nom)
    with open(chemin, "w") as f:
        f.write(contenu)
    print(f"  → {chemin}")
    return chemin


# ── Utilitaires SVG ──────────────────────────────────────────

def echelle(valeurs, v_min, v_max, px_min, px_max):
    """Convertit une valeur en coordonnée pixel."""
    if v_max == v_min:
        return px_min
    return px_min + (valeurs - v_min) / (v_max - v_min) * (px_max - px_min)


def polyline(xs, ys, couleur, epaisseur=2):
    pts = " ".join(f"{x:.1f},{y:.1f}" for x, y in zip(xs, ys))
    return f'<polyline points="{pts}" fill="none" stroke="{couleur}" stroke-width="{epaisseur}" stroke-linejoin="round" stroke-linecap="round"/>'


def grille_svg(x0, y0, x1, y1, nb_h=5, nb_v=5):
    lignes = []
    for i in range(nb_h + 1):
        y = y0 + i * (y1 - y0) / nb_h
        lignes.append(f'<line x1="{x0}" y1="{y:.1f}" x2="{x1}" y2="{y:.1f}" stroke="#E5E7EB" stroke-width="0.5"/>')
    for i in range(nb_v + 1):
        x = x0 + i * (x1 - x0) / nb_v
        lignes.append(f'<line x1="{x:.1f}" y1="{y0}" x2="{x:.1f}" y2="{y1}" stroke="#E5E7EB" stroke-width="0.5"/>')
    return "\n".join(lignes)


def axe_y_labels(vals, x0, y0, y1, nb=5):
    v_min, v_max = min(vals), max(vals)
    items = []
    for i in range(nb + 1):
        v = v_min + i * (v_max - v_min) / nb
        y = y1 - i * (y1 - y0) / nb
        items.append(f'<text x="{x0-6}" y="{y:.1f}" text-anchor="end" font-size="10" fill="#6B7280" dominant-baseline="middle">{v:.2f}</text>')
    return "\n".join(items)


def axe_x_labels(vals, y1, x0, x1, nb=None):
    if nb is None:
        nb = len(vals) - 1
    items = []
    for i, v in enumerate(vals):
        x = x0 + i * (x1 - x0) / max(nb, 1)
        items.append(f'<text x="{x:.1f}" y="{y1+14}" text-anchor="middle" font-size="10" fill="#6B7280">{v}</text>')
    return "\n".join(items)


COULEURS = ["#378ADD", "#1D9E75", "#D85A30", "#8B5CF6", "#F59E0B"]

ENTETE_SVG = '''<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}">
<rect width="{w}" height="{h}" fill="white"/>
<style>
  text {{ font-family: system-ui, sans-serif; }}
</style>
'''


# ═══════════════════════════════════════════════════════════
# FIGURE 1 : Latence des RPCs (boîtes + points)
# ═══════════════════════════════════════════════════════════

def figure_latence():
    print("\n[Figure 1] Latence des RPCs...")
    lignes, _ = lire_csv("latence_rpcs.csv")
    if not lignes:
        return

    rpcs  = ["RegisterClient", "GetGlobalModel", "GetStatus", "Heartbeat"]
    cols  = ["RegisterClient_ms", "GetGlobalModel_ms", "GetStatus_ms", "Heartbeat_ms"]
    W, H  = 680, 380
    x0, x1, y0, y1 = 70, 620, 40, 300

    # Statistiques par RPC
    stats = []
    for col in cols:
        vals = [float(l[col]) for l in lignes]
        stats.append({
            "vals": vals,
            "moy":  sum(vals) / len(vals),
            "mini": min(vals),
            "maxi": max(vals),
            "med":  sorted(vals)[len(vals)//2],
        })

    v_max = max(s["maxi"] for s in stats) * 1.1
    v_min = 0

    svg = ENTETE_SVG.format(w=W, h=H)
    svg += f'<text x="{W//2}" y="22" text-anchor="middle" font-size="14" font-weight="bold" fill="#111827">Latence des RPCs gRPC (ms)</text>\n'
    svg += grille_svg(x0, y0, x1, y1, nb_h=5, nb_v=len(rpcs)-1) + "\n"
    svg += axe_y_labels(
        [v_min + i*(v_max-v_min)/5 for i in range(6)],
        x0, y0, y1, nb=5) + "\n"
    svg += f'<text x="{x0-45}" y="{(y0+y1)//2}" text-anchor="middle" font-size="11" fill="#6B7280" transform="rotate(-90,{x0-45},{(y0+y1)//2})">Latence (ms)</text>\n'

    bw = (x1 - x0) / len(rpcs)

    for i, (rpc, stat) in enumerate(zip(rpcs, stats)):
        cx = x0 + i * bw + bw / 2
        couleur = COULEURS[i]

        # Barre (moyenne)
        y_moy  = y1 - (stat["moy"] - v_min) / (v_max - v_min) * (y1 - y0)
        y_base = y1
        svg += f'<rect x="{cx-bw*0.3:.1f}" y="{y_moy:.1f}" width="{bw*0.6:.1f}" height="{y_base-y_moy:.1f}" fill="{couleur}" opacity="0.7" rx="3"/>\n'

        # Ligne min-max
        y_min_ = y1 - (stat["mini"] - v_min) / (v_max - v_min) * (y1 - y0)
        y_max_ = y1 - (stat["maxi"] - v_min) / (v_max - v_min) * (y1 - y0)
        svg += f'<line x1="{cx}" y1="{y_min_:.1f}" x2="{cx}" y2="{y_max_:.1f}" stroke="{couleur}" stroke-width="1.5"/>\n'
        svg += f'<line x1="{cx-8}" y1="{y_min_:.1f}" x2="{cx+8}" y2="{y_min_:.1f}" stroke="{couleur}" stroke-width="1.5"/>\n'
        svg += f'<line x1="{cx-8}" y1="{y_max_:.1f}" x2="{cx+8}" y2="{y_max_:.1f}" stroke="{couleur}" stroke-width="1.5"/>\n'

        # Valeur moyenne
        svg += f'<text x="{cx:.1f}" y="{y_moy-6:.1f}" text-anchor="middle" font-size="10" fill="{couleur}" font-weight="bold">{stat["moy"]:.2f}</text>\n'

        # Label X
        label = rpc.replace("Client","").replace("Global","").replace("Local","")
        svg += f'<text x="{cx:.1f}" y="{y1+18}" text-anchor="middle" font-size="10" fill="#374151">{label}</text>\n'

    # Axes
    svg += f'<line x1="{x0}" y1="{y0}" x2="{x0}" y2="{y1}" stroke="#9CA3AF" stroke-width="1"/>\n'
    svg += f'<line x1="{x0}" y1="{y1}" x2="{x1}" y2="{y1}" stroke="#9CA3AF" stroke-width="1"/>\n'

    # Légende
    svg += f'<text x="{W//2}" y="{H-15}" text-anchor="middle" font-size="10" fill="#6B7280">Barre = moyenne | Trait vertical = [min, max] | 30 mesures par RPC</text>\n'
    svg += '</svg>'
    sauver_svg("fig1_latence_rpcs.svg", svg)


# ═══════════════════════════════════════════════════════════
# FIGURE 2 : Convergence FedAvg
# ═══════════════════════════════════════════════════════════

def figure_convergence():
    print("\n[Figure 2] Convergence FedAvg...")
    lignes, _ = lire_csv("convergence_fedavg.csv")
    if not lignes:
        return

    rounds  = [int(l["round"]) for l in lignes]
    ws      = [float(l["w"]) for l in lignes]
    bs      = [float(l["b"]) for l in lignes]
    erreurs = [float(l["erreur_totale"]) for l in lignes]

    W, H = 680, 400
    x0, x1, y0, y1 = 70, 620, 40, 300

    v_min = min(min(ws), min(bs), 0) * 0.98
    v_max = max(max(ws), max(bs)) * 1.02

    svg = ENTETE_SVG.format(w=W, h=H)
    svg += f'<text x="{W//2}" y="22" text-anchor="middle" font-size="14" font-weight="bold" fill="#111827">Convergence FedAvg — 3 clients hétérogènes</text>\n'
    svg += grille_svg(x0, y0, x1, y1) + "\n"

    # Lignes cibles
    y_cible_w = y1 - (2.0 - v_min) / (v_max - v_min) * (y1 - y0)
    y_cible_b = y1 - (1.0 - v_min) / (v_max - v_min) * (y1 - y0)
    svg += f'<line x1="{x0}" y1="{y_cible_w:.1f}" x2="{x1}" y2="{y_cible_w:.1f}" stroke="#378ADD" stroke-width="0.8" stroke-dasharray="5 3" opacity="0.5"/>\n'
    svg += f'<line x1="{x0}" y1="{y_cible_b:.1f}" x2="{x1}" y2="{y_cible_b:.1f}" stroke="#1D9E75" stroke-width="0.8" stroke-dasharray="5 3" opacity="0.5"/>\n'
    svg += f'<text x="{x1+4}" y="{y_cible_w:.1f}" font-size="9" fill="#378ADD" dominant-baseline="middle">w=2.0</text>\n'
    svg += f'<text x="{x1+4}" y="{y_cible_b:.1f}" font-size="9" fill="#1D9E75" dominant-baseline="middle">b=1.0</text>\n'

    def px(v):
        return y1 - (v - v_min) / (v_max - v_min) * (y1 - y0)

    def rx(r):
        n = len(rounds)
        return x0 + r / max(n-1,1) * (x1 - x0)

    # Courbes w et b
    svg += polyline([rx(r) for r in rounds], [px(w) for w in ws], "#378ADD", 2) + "\n"
    svg += polyline([rx(r) for r in rounds], [px(b) for b in bs], "#1D9E75", 2) + "\n"

    # Points
    for r, w, b in zip(rounds, ws, bs):
        svg += f'<circle cx="{rx(r):.1f}" cy="{px(w):.1f}" r="3.5" fill="#378ADD"/>\n'
        svg += f'<circle cx="{rx(r):.1f}" cy="{px(b):.1f}" r="3.5" fill="#1D9E75"/>\n'

    # Axes et labels
    svg += f'<line x1="{x0}" y1="{y0}" x2="{x0}" y2="{y1}" stroke="#9CA3AF" stroke-width="1"/>\n'
    svg += f'<line x1="{x0}" y1="{y1}" x2="{x1}" y2="{y1}" stroke="#9CA3AF" stroke-width="1"/>\n'
    svg += axe_y_labels([v_min + i*(v_max-v_min)/5 for i in range(6)], x0, y0, y1) + "\n"
    for r in rounds:
        svg += f'<text x="{rx(r):.1f}" y="{y1+14}" text-anchor="middle" font-size="10" fill="#6B7280">{r}</text>\n'
    svg += f'<text x="{(x0+x1)//2}" y="{H-28}" text-anchor="middle" font-size="11" fill="#374151">Round</text>\n'
    svg += f'<text x="{x0-45}" y="{(y0+y1)//2}" text-anchor="middle" font-size="11" fill="#374151" transform="rotate(-90,{x0-45},{(y0+y1)//2})">Valeur du paramètre</text>\n'

    # Légende
    lg_y = H - 45
    for i, (label, col) in enumerate([("w (cible=2.0)","#378ADD"),("b (cible=1.0)","#1D9E75")]):
        lx = 120 + i * 200
        svg += f'<line x1="{lx}" y1="{lg_y+5}" x2="{lx+24}" y2="{lg_y+5}" stroke="{col}" stroke-width="2"/>\n'
        svg += f'<circle cx="{lx+12}" cy="{lg_y+5}" r="3.5" fill="{col}"/>\n'
        svg += f'<text x="{lx+30}" y="{lg_y+9}" font-size="11" fill="#374151">{label}</text>\n'
    svg += f'<line x1="520" y1="{lg_y+5}" x2="544" y2="{lg_y+5}" stroke="#9CA3AF" stroke-width="1" stroke-dasharray="5 3"/>\n'
    svg += f'<text x="550" y="{lg_y+9}" font-size="11" fill="#6B7280">Cible</text>\n'

    svg += '</svg>'
    sauver_svg("fig2_convergence_fedavg.svg", svg)


# ═══════════════════════════════════════════════════════════
# FIGURE 3 : Horloges de Lamport
# ═══════════════════════════════════════════════════════════

def figure_lamport():
    print("\n[Figure 3] Horloges de Lamport...")
    lignes, _ = lire_csv("lamport_ordre_causal.csv")
    if not lignes:
        return

    clients  = sorted(set(l["client"] for l in lignes))
    rpcs     = sorted(set(l["rpc"] for l in lignes))
    W, H     = 680, 380
    x0, x1  = 80, 620
    y0, y1  = 50, 300

    # LC max pour l'axe Y
    lc_max = max(int(l["lc_apres"]) for l in lignes) + 2

    def py(lc):
        return y1 - int(lc) / lc_max * (y1 - y0)

    def px_client(c):
        idx = clients.index(c)
        return x0 + idx * (x1 - x0) / max(len(clients)-1, 1)

    svg = ENTETE_SVG.format(w=W, h=H)
    svg += f'<text x="{W//2}" y="25" text-anchor="middle" font-size="14" font-weight="bold" fill="#111827">Horloges logiques de Lamport — Ordre causal</text>\n'

    # Lignes verticales par client
    for c in clients:
        cx = px_client(c)
        svg += f'<line x1="{cx:.1f}" y1="{y0}" x2="{cx:.1f}" y2="{y1}" stroke="#E5E7EB" stroke-width="1" stroke-dasharray="4 2"/>\n'
        svg += f'<text x="{cx:.1f}" y="{y0-10}" text-anchor="middle" font-size="12" font-weight="500" fill="#374151">{c}</text>\n'

    # Grille Y
    for i in range(0, lc_max, max(1, lc_max//6)):
        y = py(i)
        svg += f'<line x1="{x0-5}" y1="{y:.1f}" x2="{x1}" y2="{y:.1f}" stroke="#F3F4F6" stroke-width="0.5"/>\n'
        svg += f'<text x="{x0-8}" y="{y:.1f}" text-anchor="end" font-size="9" fill="#9CA3AF" dominant-baseline="middle">LC={i}</text>\n'

    # Événements et flèches
    prev_evt = {}
    for ligne in lignes:
        c       = ligne["client"]
        rpc     = ligne["rpc"]
        lc_av   = int(ligne["lc_avant"])
        lc_ap   = int(ligne["lc_apres"])
        ok      = ligne["ordre_respecte"] == "True"
        cx      = px_client(c)
        couleur = COULEURS[clients.index(c)]

        y_av = py(lc_av)
        y_ap = py(lc_ap)

        # Trait vertical (progression de l'horloge)
        svg += f'<line x1="{cx:.1f}" y1="{y_av:.1f}" x2="{cx:.1f}" y2="{y_ap:.1f}" stroke="{couleur}" stroke-width="1.5"/>\n'

        # Point d'événement
        forme = "●" if "GetGlobal" in rpc else "■"
        svg += f'<circle cx="{cx:.1f}" cy="{y_ap:.1f}" r="5" fill="{couleur}" stroke="white" stroke-width="1.5"/>\n'

        # Label LC
        svg += f'<text x="{cx+9:.1f}" y="{y_ap:.1f}" font-size="9" fill="{couleur}" dominant-baseline="middle">{lc_ap}</text>\n'

    # Axes
    svg += f'<line x1="{x0}" y1="{y0}" x2="{x0}" y2="{y1}" stroke="#9CA3AF" stroke-width="1"/>\n'
    svg += f'<line x1="{x0}" y1="{y1}" x2="{x1}" y2="{y1}" stroke="#9CA3AF" stroke-width="1"/>\n'

    # Légende
    nb_ok = sum(1 for l in lignes if l["ordre_respecte"]=="True")
    svg += f'<text x="{W//2}" y="{H-20}" text-anchor="middle" font-size="11" fill="#1D9E75">✓ {nb_ok}/{len(lignes)} événements respectent l\'ordre causal LC(avant) &lt; LC(après)</text>\n'
    svg += f'<text x="{W//2}" y="{H-5}" text-anchor="middle" font-size="10" fill="#6B7280">● GetGlobalModel  ■ SendLocalUpdate</text>\n'
    svg += '</svg>'
    sauver_svg("fig3_lamport.svg", svg)


# ═══════════════════════════════════════════════════════════
# FIGURE 4 : Scalabilité
# ═══════════════════════════════════════════════════════════

def figure_scalabilite():
    print("\n[Figure 4] Scalabilité...")
    lignes, _ = lire_csv("scalabilite.csv")
    if not lignes:
        return

    nb_clients = [int(l["nb_clients"]) for l in lignes]
    moyennes   = [float(l["duree_moy_ms"]) for l in lignes]
    stds       = [float(l["duree_std_ms"]) for l in lignes]

    W, H = 540, 360
    x0, x1, y0, y1 = 70, 460, 40, 270

    v_min = 0
    v_max = max(moyennes) * 1.2

    def px(i):
        return x0 + i * (x1 - x0) / max(len(nb_clients)-1, 1)
    def py(v):
        return y1 - (v - v_min) / (v_max - v_min) * (y1 - y0)

    svg = ENTETE_SVG.format(w=W, h=H)
    svg += f'<text x="{W//2}" y="22" text-anchor="middle" font-size="14" font-weight="bold" fill="#111827">Impact du nombre de clients sur la durée d\'un round</text>\n'
    svg += grille_svg(x0, y0, x1, y1, nb_h=4, nb_v=len(nb_clients)-1) + "\n"

    # Barres avec barre d'erreur
    bw = (x1 - x0) / len(nb_clients) * 0.5
    for i, (nb, moy, std) in enumerate(zip(nb_clients, moyennes, stds)):
        cx = px(i)
        y_moy  = py(moy)
        y_base = y1
        svg += f'<rect x="{cx-bw/2:.1f}" y="{y_moy:.1f}" width="{bw:.1f}" height="{y_base-y_moy:.1f}" fill="{COULEURS[i]}" opacity="0.8" rx="4"/>\n'
        y_top = py(moy + std)
        y_bot = py(max(moy - std, 0))
        svg += f'<line x1="{cx}" y1="{y_top:.1f}" x2="{cx}" y2="{y_bot:.1f}" stroke="#374151" stroke-width="1.5"/>\n'
        svg += f'<line x1="{cx-6}" y1="{y_top:.1f}" x2="{cx+6}" y2="{y_top:.1f}" stroke="#374151" stroke-width="1.5"/>\n'
        svg += f'<line x1="{cx-6}" y1="{y_bot:.1f}" x2="{cx+6}" y2="{y_bot:.1f}" stroke="#374151" stroke-width="1.5"/>\n'
        svg += f'<text x="{cx:.1f}" y="{y_moy-8:.1f}" text-anchor="middle" font-size="11" fill="{COULEURS[i]}" font-weight="bold">{moy:.0f}ms</text>\n'
        svg += f'<text x="{cx:.1f}" y="{y1+15}" text-anchor="middle" font-size="11" fill="#374151">{nb} client{"s" if nb>1 else ""}</text>\n'

    svg += f'<line x1="{x0}" y1="{y0}" x2="{x0}" y2="{y1}" stroke="#9CA3AF" stroke-width="1"/>\n'
    svg += f'<line x1="{x0}" y1="{y1}" x2="{x1}" y2="{y1}" stroke="#9CA3AF" stroke-width="1"/>\n'
    svg += axe_y_labels([v_min + i*(v_max-v_min)/4 for i in range(5)], x0, y0, y1, nb=4) + "\n"
    svg += f'<text x="{x0-45}" y="{(y0+y1)//2}" text-anchor="middle" font-size="11" fill="#374151" transform="rotate(-90,{x0-45},{(y0+y1)//2})">Durée (ms)</text>\n'
    svg += f'<text x="{W//2}" y="{H-8}" text-anchor="middle" font-size="10" fill="#6B7280">Barre d\'erreur = écart-type sur 5 rounds</text>\n'
    svg += '</svg>'
    sauver_svg("fig4_scalabilite.svg", svg)


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("="*55)
    print("  Génération des figures — INF4218")
    print("="*55)

    figure_latence()
    figure_convergence()
    figure_lamport()
    figure_scalabilite()

    print("\n✅ Figures générées dans experiments/figures/")
    print("   Formats : SVG (ouvrable dans tout navigateur)")