#!/bin/bash
set -e

cd /app/api
gunicorn --bind 127.0.0.1:5000 --workers 2 --timeout 120 app:app &

nginx -g 'daemon off;'
