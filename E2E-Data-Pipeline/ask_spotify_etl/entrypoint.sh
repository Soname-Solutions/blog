#!/bin/sh
luigid --background --logdir /var/log/luigi --pidfile /var/log/luigi/luigid.pid &
python ask_spotify_etl.py