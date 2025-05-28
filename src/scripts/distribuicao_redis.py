'''
Divisão de 1 % 10:
Divisão inteira: 1 // 10 = 0 (1 dividido por 10 dá 0, pois estamos considerando a parte inteira).

- Multiplicação do resultado da divisão: 0 * 10 = 0.
- Subtração para encontrar o resto: 1 - 0 = 1.
- Portanto, o resultado de 1 % 10 é 1.

Divisão de 9 % 10:
Divisão inteira: 9 // 10 = 0 (9 dividido por 10 dá 0, pois estamos considerando a parte inteira).

- Multiplicação do resultado da divisão: 0 * 10 = 0.
- Subtração para encontrar o resto: 9 - 0 = 9.
- Portanto, o resultado de 9 % 10 é 9.

Divisão de 10 % 10:
Divisão inteira: 10 // 10 = 1 (10 dividido por 10 dá 1, pois estamos considerando a parte inteira).

- Multiplicação do resultado da divisão: 1 * 10 = 10.
- Subtração para encontrar o resto: 10 - 10 = 0.
- Portanto, o resultado de 10 % 10 é 0.
	
'''
from collections import defaultdict

redis_keys = defaultdict(list)

def distribuicao_redis(total_chaves):
    chaves = [f"chave_{i+1}" for i in range(total_chaves)]
    
    for i in range(len(chaves)):
        print(str(i % 10))
        idx = (i % 10) + 1
        robo_idx = f"robo_{idx}"
        print(f"{robo_idx}: {chaves[i]}")
        redis_keys[robo_idx].append(chaves[i])
    
    print("\n===================================")
    print(f"Total de chaves: {total_chaves}")
    print("===================================")
    print("Distribuição das chaves")
    print("===================================")
    for robo_idx, chaves in redis_keys.items():
        print(f"{robo_idx}: {len(chaves)} chaves")    

distribuicao_redis(127)