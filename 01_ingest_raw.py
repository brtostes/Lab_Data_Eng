from __future__ import annotations

from pathlib import Path
import shutil
import pandas as pd

from utils import ensure_directories, PROJECT_ROOT, DATA_RAW, RAW_CSV_NAME


SOURCE_FILE = PROJECT_ROOT / RAW_CSV_NAME
TARGET_FILE = DATA_RAW / RAW_CSV_NAME


def main() -> None:
    ensure_directories()

    if not SOURCE_FILE.exists() and not TARGET_FILE.exists():
        raise FileNotFoundError(
            f'Arquivo não encontrado. Coloque {RAW_CSV_NAME} na raiz do projeto ou em data/raw/.'
        )

    if SOURCE_FILE.exists():
        shutil.copy2(SOURCE_FILE, TARGET_FILE)

    df = pd.read_csv(TARGET_FILE)
    print('Ingestão RAW concluída.')
    print(f'Arquivo salvo em: {TARGET_FILE}')
    print(f'Linhas: {df.shape[0]}')
    print(f'Colunas: {df.shape[1]}')
    print('Colunas:')
    for col in df.columns:
        print(f' - {col}')


if __name__ == '__main__':
    main()
