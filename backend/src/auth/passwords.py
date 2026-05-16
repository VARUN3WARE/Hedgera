from passlib.context import CryptContext

_pwd = CryptContext(schemes=["bcrypt"], deprecated="auto")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return _pwd.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    return _pwd.hash(password)
