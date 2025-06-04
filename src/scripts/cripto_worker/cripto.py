from cryptography.fernet import Fernet

def encript(yaml_name, encrypted_name):    
    # Gere uma chave secreta (faça isso apenas uma vez e guarde)
    key = Fernet.generate_key()
    print(f"Chave secreta {yaml_name}: {key.decode()}")  # Salve isso de forma segura!

    # Criptografa o arquivo YAML
    cipher = Fernet(key)
    with open(yaml_name, "rb") as f:
        encrypted_data = cipher.encrypt(f.read())
    with open(encrypted_name, "wb") as f:
        f.write(encrypted_data)
    print(f"Configuração criptografada salva. {yaml_name} -> {encrypted_name}")

encript("src/scripts/cripto_worker/config.dev.yaml", "src/scripts/cripto_worker/config.dev.enc")
encript("src/scripts/cripto_worker/config.prod.yaml", "src/scripts/cripto_worker/config.prod.enc")

# ultima criacao
#Chave secreta src/scripts/cripto_worker/config.dev.yaml: zR0bBT8gZWpiJ3jHgDK-tY7aSrV5RkpG3Yxqxff51vg=
#Chave secreta src/scripts/cripto_worker/config.prod.yaml: ImCszQ-84m5JyVF6OH7Gvx-7vqgijIwVArc7yiTY5hU=