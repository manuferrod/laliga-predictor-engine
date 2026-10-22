# _legacy_v1 — pipeline inicial (referencia, NO se ejecuta)

Contenido de la primera versión del pipeline TFM. Se conserva como base y
referencia de razonamiento; **no forma parte del pipeline v2 y no debe ejecutarse.**

- `scripts/`   — orquestación papermill v1 (run_stage1/2) + verificación/curvas de salida.
                 Apuntaban a parquets y notebooks que ya no existen en v2.
- `workflows/` — GitHub Actions v1 (prepare.yml, finalize.yml). Movidos aquí para
                 desactivarlos (fuera de .github/workflows no se disparan).
- `manual/`    — entradas manuales de cuotas Bet365 (b365_filled_*.csv) del flujo v1.

El pipeline v2 vive en: scripts/extractors, scripts/consolidate y notebooks/01,02.
