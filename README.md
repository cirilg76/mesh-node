# 📟 Mon nœud Meshtastic

Interface web pour piloter un nœud [Meshtastic](https://meshtastic.org) branché en USB sur un Raspberry Pi
(testé avec un Heltec V3, firmware 2.7). Aucune dépendance côté navigateur à installer : un serveur Python
(bibliothèque standard + bibliothèque officielle `meshtastic`) et une seule page HTML.

## Fonctions

- **Messages** : canaux et messages directs, accusés de réception, réponses (↩) et réactions emoji, compteur de non-lus.
- **Nœuds** : liste avec SNR et barre de signal, distance, traceroute, demande de position et de télémétrie, favoris,
  badge « nouveau ». Un clic sur un nœud ouvre son **historique** (SNR, RSSI, sauts, batterie, tension sur 24 h / 7 j / 30 j).
- **Carte** (Leaflet + OpenStreetMap) : nœuds colorés selon le signal, liaisons directes et chemins de traceroute,
  distance et cap depuis ton nœud.
- **Paquets** : tout ce que reçoit le nœud, en direct.
- **📊 Stats** : batterie, tension, occupation du canal et temps d'émission de ton nœud ; nœuds entendus par jour,
  activité par heure, nœuds reçus en direct triés par distance, plus actifs, nouveaux nœuds.
  Comparaison facultative avec un récepteur SDR (voir plus bas).
- **🧪 Outils** : test de portée (série de messages avec accusé), sauvegarde / restauration de la configuration en JSON,
  notifications sur le téléphone via [ntfy](https://ntfy.sh) ou Telegram (messages, nouveaux nœuds, réactions).
- **⚙️ Réglages** : nom, région et préréglage LoRa, puissance, sauts, rôle, position fixe, canaux (clé, QR code de partage).
- **Connexion protégée** par identifiant / mot de passe (PBKDF2, cookie de session HttpOnly, anti-force brute).

## Installation

```bash
git clone https://github.com/cirilg76/mesh-node.git
cd mesh-node
bash install.sh
```

Le script crée l'environnement Python, ajoute l'utilisateur au groupe `dialout` (port série), copie
`config.example.json` en `config.json` et installe le service systemd `mesh-node` (port 8089).

Au premier démarrage, un identifiant `admin` et un mot de passe aléatoire sont écrits dans
`data/mot-de-passe-initial.txt`. Change-les puis supprime ce fichier :

```bash
venv/bin/python server.py passwd
rm data/mot-de-passe-initial.txt
```

Le port série est détecté automatiquement ; sinon : variable d'environnement `NODE_DEV=/dev/ttyUSB0`.
Port web : `NODE_PORT` (8089 par défaut).

## Configuration (`config.json`, non publié)

```json
{
  "home": {"name": "Chez moi", "lat": 48.8584, "lon": 2.2945, "alt": 35},
  "lora_mesh": {"db": "~/lora-mesh/data/mesh.db", "port": 8088}
}
```

- `home` : ajoute un bouton pour remplir la position fixe et centre la carte.
- `lora_mesh` *(facultatif)* : base SQLite et port d'une appli d'écoute Meshtastic par SDR, pour comparer
  ce que reçoivent le nœud et le SDR. Supprime la clé si tu n'en as pas.

## Données

Tout est stocké localement dans `data/` (ignoré par git) : `node.db` (messages, historique du signal et de la
télémétrie, conservé 90 jours), `auth.json` (mot de passe haché), `sessions.json`.

## Sécurité

Le site est servi en HTTP : parfait sur le réseau local. Pour un accès depuis Internet, passe par un VPN
(Tailscale, WireGuard) ou un reverse proxy HTTPS plutôt que d'ouvrir le port.

Sur ntfy.sh public, toute personne qui connaît le sujet peut lire les notifications : choisis un sujet long
et aléatoire (bouton 🎲 dans l'interface).

## Licence

MIT
