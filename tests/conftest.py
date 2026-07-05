"""
Configuration pytest partagée.

Force PySpark à lancer ses workers avec le même interpréteur Python que celui
qui exécute les tests. Évite l'erreur Windows "Cannot run program python.exe /
Le fichier spécifié est introuvable" quand PYSPARK_PYTHON pointe vers une
version de Python désinstallée. Fonctionne aussi tel quel en intégration
continue (GitHub Actions).
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
