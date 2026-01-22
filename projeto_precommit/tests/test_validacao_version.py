import sys

MIN_VERSION = (3, 9, 0)
HOMOLOGATED = (3, 12, 7)


def validate_python_version():
    v = sys.version_info[:3]
    current_str = f"{v[0]}.{v[1]}.{v[2]}"

    print("Verificando ambiente Python...")
    print(f"Versão Detectada: {current_str}")

    if v < MIN_VERSION:
        print("ERRO: Versão do Python muito antiga.")
        print(f"Requerido: >={MIN_VERSION[0]}.{MIN_VERSION[1]}.{MIN_VERSION[2]}")
        sys.exit(1)

    if v != HOMOLOGATED:
        print("ERRO: Versão do Python diferente da homologada.")
        print(f"Homologado: {HOMOLOGATED[0]}.{HOMOLOGATED[1]}.{HOMOLOGATED[2]}")
        sys.exit(1)

    print(f"Ambiente Válido! Python {current_str} está homologado.")
    sys.exit(0)


if __name__ == "__main__":
    validate_python_version()
