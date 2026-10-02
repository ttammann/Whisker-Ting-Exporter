# Runbook (example)

The real runbook lives in the private overlay and names the real hosts. Steps for one host, the host with
physical access first (design 13.3):

1. `python3 tools/overlay.py check` and `pytest overlay/tests`.
2. `python3 tools/overlay.py bundle <host>`: prints the commands; run them by hand, one at a time.
3. After `docker compose up -d --dry-run`: only the Ting containers may be recreated.
4. Afterwards: `docker exec ting-exporter python -m ting_exporter compare-stores <store A> <store B>`.
