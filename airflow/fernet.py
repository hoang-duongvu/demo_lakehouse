from cryptography.fernet import Fernet
fernet_key = Fernet.generate_key().decode()
print(fernet_key)