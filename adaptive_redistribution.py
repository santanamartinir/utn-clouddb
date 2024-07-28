from collections import defaultdict

def selective_broadcast(partitions, heavy_hitters):
    broadcasted_tuples = defaultdict(list)  # Diccionario para almacenar tuplas redistribuidas
    for partition, tuples in partitions.items():  # Iteramos sobre cada partición y sus tuplas
        for key, value in tuples:  # Iteramos sobre cada tupla en la partición
            if key in heavy_hitters:  # Si la clave es un heavy hitter
                for p in partitions:  # Redistribuimos la tupla a todas las particiones
                    broadcasted_tuples[p].append((key, value))
            else:  # Si no es un heavy hitter
                broadcasted_tuples[partition].append((key, value))  # Mantenemos la tupla en su partición original
    print(f"Tuplas redistribuidas: {dict(broadcasted_tuples)}")
    return broadcasted_tuples  # Devolvemos las tuplas redistribuidas