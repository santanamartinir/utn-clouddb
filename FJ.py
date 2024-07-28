import partition as p
import heavy_hitter_detection as hh
import adaptive_redistribution as ar

def read_data(file_name):
    with open(file_name, 'r') as f:
        data = [int(line.strip()) for line in f]
    return [(d, str(d)) for d in data]

def flow_join(data1, data2, num_partitions, k, data_structure):
    partitions1 = p.partition_data(data1, num_partitions)  # Particionamos data1
    partitions2 = p.partition_data(data2, num_partitions)  # Particionamos data2
    
    ss1 = hh.SpaceSaving(k, data_structure)  # Creamos instancia de SpaceSaving para data1
    ss2 = hh.SpaceSaving(k, data_structure)  # Creamos instancia de SpaceSaving para data2
    
    ss1.process([key for key, value in data1])  # Procesamos claves de data1
    ss2.process([key for key, value in data2])  # Procesamos claves de data2
    
    # heavy_hitters1 = set(key for key, count in ss1.get_heavy_hitters())  # Obtenemos heavy hitters de data1
    # heavy_hitters2 = set(key for key, count in ss2.get_heavy_hitters())  # Obtenemos heavy hitters de data2
    
    heavy_hitters1 = set(ss1.counters.keys())
    heavy_hitters2 = set(ss2.counters.keys())

    print(f"Heavy hitters de data1: {heavy_hitters1}")
    print(f"Heavy hitters de data2: {heavy_hitters2}")
    
    heavy_hitters = heavy_hitters1.union(heavy_hitters2)  # Unimos heavy hitters de data1 y data2
    print(f"Heavy hitters combinados: {heavy_hitters}")

    redistributed_partitions1 = ar.selective_broadcast(partitions1, heavy_hitters)  # Redistribuimos particiones de data1
    redistributed_partitions2 = ar.selective_broadcast(partitions2, heavy_hitters)  # Redistribuimos particiones de data2
    
    joined_data =  set()  # Conjunto para almacenar los resultados del join
    for partition in range(num_partitions):
        # tuples1 = {key: value for key, value in redistributed_partitions1[partition]}  # Diccionario de tuplas de partición 1
        # tuples2 = {key: value for key, value in redistributed_partitions2[partition]}  # Diccionario de tuplas de partición 2
        tuples1 = dict(redistributed_partitions1[partition])
        tuples2 = dict(redistributed_partitions2[partition])
        for key in tuples1:
            if key in tuples2:  # Si la clave está en ambas particiones
                joined_data.add((key, tuples1[key], tuples2[key]))  # Añadimos la tupla resultante al join
    
    print(f"Datos del join: {joined_data}") 
    return joined_data  # Devolvemos los datos del join

# Ejemplo
# data1_file = "zipf_1_1p0_1000_10.txt"
# data2_file = "zipf_2_1p0_1000_10.txt"  # Usando el mismo archivo para ambos conjuntos de datos
# data1 = read_data(data1_file)
# data2 = read_data(data2_file)

data1 = [(1, 'A'), (1, 'B'), (161, 'C'), (17, 'D'), (30, 'E'), (3, 'F'), (1, 'G'), (90, 'H'), (91, 'I'), (614, 'J')]
data2 = [(1, 'K'), (4, 'L'), (26, 'M'), (539, 'N'), (1, 'O'), (15, 'P'), (1, 'Q'), (8, 'R'), (8, 'S'), (376, 'T')]
    
num_partitions = 3
k = 2
data_structure = 'heap'
joined_data = flow_join(data1, data2, num_partitions, k, data_structure)
print(f"Resultado final del join: {joined_data}")