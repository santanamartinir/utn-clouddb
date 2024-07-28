from collections import defaultdict

def partition_data(data, num_partitions):
    partitions = defaultdict(list)  # Diccionario para almacenar particiones
    for key, value in data:
        partition = key % num_partitions  # Asignamos partición basada en la clave
        partitions[partition].append((key, value))  # Añadimos la tupla a la partición correspondiente
    print(f"Particiones después de particionar {data}: {dict(partitions)}")
    return partitions  # Devolvemos las particiones