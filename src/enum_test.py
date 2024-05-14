from enum import Enum, IntEnum

class Fonte(Enum):   
    SIMCAR_COM_CPF = (1, "Simcar com CPF")
    AGROTOOLS_FS = (2, "Agrotools FS")
    SIGEF = (3, "SIGEF")
    MANUAL = (4, "Manual") 

print(Fonte.AGROTOOLS_FS.value)
print(Fonte.AGROTOOLS_FS.name)
print(Fonte.__members__.values())
xxx = ', '.join([ f"{str(val)} {desc}" for val, desc in list(map(lambda x: x.value, Fonte))])
print(xxx)
ids = ', '.join([ str(val) for val, desc in list(map(lambda x: x.value, Fonte))])
print(ids)