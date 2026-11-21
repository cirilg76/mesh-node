#!/bin/bash
# Installation de « Mon nœud Meshtastic » (contrôle web d'un nœud branché en USB, port 8089).
# À lancer une fois depuis le dossier de l'appli :   bash install.sh
set -e
APP="$(cd "$(dirname "$0")" && pwd)"
USR="$(whoami)"
cd "$APP"
echo "=== 1/3 Environnement Python ==="
[ -d venv ] || python3 -m venv venv
venv/bin/pip install -q -r requirements.txt
[ -f config.json ] || cp config.example.json config.json
echo "=== 2/3 Accès au port série (groupe dialout) ==="
sudo usermod -aG dialout "$USR"
echo "=== 3/3 Service web (port 8089, démarrage automatique) ==="
sudo tee /etc/systemd/system/mesh-node.service >/dev/null <<UNIT
[Unit]
Description=Mon noeud Meshtastic - controle web (port 8089)
After=network-online.target

[Service]
User=$USR
SupplementaryGroups=dialout
WorkingDirectory=$APP
ExecStart=$APP/venv/bin/python $APP/server.py
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
UNIT
sudo systemctl daemon-reload
sudo systemctl enable --now mesh-node.service
sleep 3
systemctl --no-pager --lines=3 status mesh-node.service || true
echo
echo "Identifiants de connexion initiaux : $APP/data/mot-de-passe-initial.txt"
echo "Changer le mot de passe :            cd $APP && venv/bin/python server.py passwd"
echo "OK → ouvre http://$(hostname -I | awk '{print $1}'):8089"
