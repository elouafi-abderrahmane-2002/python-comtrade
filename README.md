# ⚡ Pipeline COMTRADE — Données IED & Relais de Protection

Les Intelligent Electronic Devices (IEDs) et les enregistreurs de perturbations
dans les postes haute tension génèrent des fichiers COMTRADE à chaque événement :
défaut, déclenchement de protection, oscillographie. Ces fichiers contiennent
des signaux analogiques (courants, tensions) et numériques (états des organes)
horodatés à la microseconde — mais dans un format binaire propriétaire illisible
directement par les outils d'analyse classiques.

Ce projet construit le pipeline complet : du fichier `.cfg`/`.dat` COMTRADE brut
jusqu'à une base de données structurée, prête pour l'analyse et le reporting.

---

## Format COMTRADE — structure des fichiers

```
  Événement IED (ex: déclenchement disjoncteur HV)
          │
          │  Génère 2-3 fichiers liés
          ▼
  ┌────────────────────────────────────────────────────────┐
  │  event_20240315_143022.cfg   ← fichier de configuration│
  │                                                        │
  │  [Station_name,rec_dev_id,rev_year]                    │
  │  [nA,nD]  ← nombre de canaux analogiques / numériques  │
  │  [An,ch_id,ph,ccbm,uu,a,b,skew,min,max,primary,second]│
  │  [Dn,ch_id,ph,ccbm,y]                                  │
  │  [lf]  ← fréquence nominale (50 Hz Maroc)              │
  │  [nrates]                                               │
  │  [samp,endsamp]                                        │
  │  [dd/mm/yyyy,hh:mm:ss.ssssss]  ← start timestamp       │
  │  [dd/mm/yyyy,hh:mm:ss.ssssss]  ← trigger timestamp     │
  │  [filetype]  ← ASCII ou BINARY                         │
  └────────────────────────────────────────────────────────┘
  ┌────────────────────────────────────────────────────────┐
  │  event_20240315_143022.dat   ← données brutes          │
  │                                                        │
  │  n,timestamp,IA,IB,IC,UA,UB,UC,...,TRIP_A,TRIP_B,...  │
  │  (canaux analogiques + états digitaux par sample)      │
  └────────────────────────────────────────────────────────┘
```

---

## Pipeline de traitement — du brut au structuré

```
  Dossier source
  /data/raw/comtrade/YYYY-MM/
  ├── event_001.cfg + event_001.dat
  ├── event_002.cfg + event_002.dat
  └── ...
          │
          │  ÉTAPE 1 : Découverte et parsing
          ▼
  ┌──────────────────────────────────────────────────────┐
  │   ComtradeIngester.scan_and_parse()                  │
  │                                                      │
  │   Pour chaque paire .cfg/.dat :                      │
  │   - Lire avec python-comtrade (IEEE C37.111)         │
  │   - Extraire métadonnées (station, device, timestamp)│
  │   - Convertir canaux → pandas DataFrame              │
  │   - Valider intégrité (sample count, NaN, overflow)  │
  └────────────────┬─────────────────────────────────────┘
                   │
                   │  ÉTAPE 2 : Normalisation
                   ▼
  ┌──────────────────────────────────────────────────────┐
  │   ComtradeNormalizer.transform()                     │
  │                                                      │
  │   - Appliquer facteurs a/b (valeurs physiques)       │
  │     val_physique = a * val_brute + b                 │
  │   - Convertir timestamps → UTC                       │
  │   - Aligner les fréquences d'échantillonnage         │
  │   - Détecter les canaux saturés (min/max dépassés)   │
  └────────────────┬─────────────────────────────────────┘
                   │
                   │  ÉTAPE 3 : Stockage structuré
                   ▼
  ┌──────────────────────────────────────────────────────┐
  │   PostgreSQL                                         │
  │                                                      │
  │   ┌──────────────┐    ┌─────────────────────────┐   │
  │   │ events       │    │ waveforms                │   │
  │   │ (métadonnées)│    │ (signaux horodatés)      │   │
  │   │              │    │                          │   │
  │   │ event_id     │◄───│ event_id (FK)            │   │
  │   │ station      │    │ timestamp_us             │   │
  │   │ device_id    │    │ channel_name             │   │
  │   │ trigger_time │    │ value_physical           │   │
  │   │ fault_type   │    │ unit                     │   │
  │   └──────────────┘    └─────────────────────────┘   │
  └──────────────────────────────────────────────────────┘
```

---

## Code : parsing COMTRADE → DataFrame structuré

```python
import comtrade
import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import create_engine

class ComtradeIngester:

    def __init__(self, db_url: str):
        self.engine = create_engine(db_url)

    def parse_file(self, cfg_path: str) -> dict:
        """
        Parse un fichier COMTRADE (format IEEE C37.111-2011).
        Retourne métadonnées + DataFrame des signaux.
        """
        rec = comtrade.load(cfg_path)

        # Métadonnées de l'événement
        metadata = {
            'station_name':  rec.station_name,
            'rec_dev_id':    rec.rec_dev_id,
            'trigger_time':  rec.trigger_timestamp,
            'start_time':    rec.start_timestamp,
            'frequency':     rec.frequency,
            'sample_rate':   rec.cfg.sample_rates[0][0],
            'n_analog':      rec.analog_count,
            'n_digital':     rec.status_count,
        }

        # Canaux analogiques (IA, IB, IC, UA, UB, UC...)
        analog_data = {'time_s': rec.time}
        for i, ch_id in enumerate(rec.analog_channel_ids):
            # Application des facteurs de calibration (a*x + b)
            analog_data[ch_id] = rec.analog[i]

        df_analog = pd.DataFrame(analog_data)
        df_analog['timestamp_us'] = (
            df_analog['time_s'] * 1_000_000
        ).astype(int)

        # Canaux numériques (TRIP_A, TRIP_B, CLOSE_CB...)
        digital_data = {'time_s': rec.time}
        for i, ch_id in enumerate(rec.status_channel_ids):
            digital_data[ch_id] = rec.status[i]

        df_digital = pd.DataFrame(digital_data)

        # Validation qualité
        n_nan_analog = df_analog.isna().sum().sum()
        if n_nan_analog > 0:
            print(f"⚠️  {n_nan_analog} valeurs manquantes détectées dans {cfg_path}")

        return {
            'metadata': metadata,
            'analog':   df_analog,
            'digital':  df_digital
        }

    def ingest_directory(self, raw_dir: str):
        """
        Scanne un répertoire, parse tous les .cfg et charge en base.
        """
        cfg_files = list(Path(raw_dir).glob('**/*.cfg'))
        print(f"📁 {len(cfg_files)} fichiers COMTRADE trouvés")

        for cfg_path in cfg_files:
            try:
                data = self.parse_file(str(cfg_path))
                self._store(data)
                print(f"  ✅ {cfg_path.name} → {len(data['analog'])} samples")
            except Exception as e:
                print(f"  ❌ {cfg_path.name} → Erreur: {e}")

    def _store(self, data: dict):
        # Insérer les métadonnées de l'événement
        df_meta = pd.DataFrame([data['metadata']])
        df_meta.to_sql('events', self.engine,
                       if_exists='append', index=False)

        # Insérer les formes d'onde (format long = plus flexible pour l'analyse)
        df_long = data['analog'].melt(
            id_vars=['time_s', 'timestamp_us'],
            var_name='channel_name',
            value_name='value_physical'
        )
        df_long.to_sql('waveforms', self.engine,
                       if_exists='append', index=False)
```

---

## Règles de qualité des données documentées

```yaml
# data_quality_rules.yaml — documenté et versionné avec le pipeline
validation_rules:
  analog_channels:
    - rule: no_overflow
      check: "abs(value) < channel_max * 1.05"
      action: flag_and_log
    - rule: no_flat_signal
      check: "std(channel_values) > 0.001"
      action: warn          # signal plat peut indiquer capteur HS
    - rule: no_missing
      check: "nan_count == 0"
      action: interpolate_linear  # interpolation si < 5% manquant

  digital_channels:
    - rule: valid_states
      check: "value in [0, 1]"
      action: reject        # état non binaire = donnée corrompue

  metadata:
    - rule: trigger_after_start
      check: "trigger_time >= start_time"
      action: reject
    - rule: known_station
      check: "station_name in registry.stations"
      action: warn_and_accept
```

---

## Ce que j'ai appris

Le facteur de calibration `a*x + b` dans le fichier .cfg est critique.
Les valeurs brutes dans le `.dat` sont des entiers — les valeurs physiques
(courant en ampères, tension en kV) ne sont obtenues qu'après application
de ce facteur. Sur les équipements HV, oublier ce facteur donne des courants
apparents de 32000 A là où il y a en réalité 320 A. Ce genre d'erreur silencieuse
peut fausser toute une analyse de défaut si on ne la détecte pas en amont.

---

*Projet réalisé dans le cadre de ma formation ingénieur — ENSET Mohammedia*
*Par **Abderrahmane Elouafi** · [LinkedIn](https://www.linkedin.com/in/abderrahmane-elouafi-43226736b/) · [Portfolio](https://my-first-porfolio-six.vercel.app/)*
