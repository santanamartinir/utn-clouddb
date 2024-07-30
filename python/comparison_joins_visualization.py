import matplotlib.pyplot as plt
import numpy as np

# Local data for Flow-Join and Hash Join

hash_join_no_skew = {
    'process_probe': np.max([0.258266, 0.251562, 0.255621, 0.257824])
}

flow_join_no_skew = {
    'detect_skew': 0.097291 / 4,
    'process_probe': np.max([0.21901, 0.224288, 0.225726, 0.227458])
}

hash_join_zipf = {
    'process_probe': np.max([0.165474, 0.376359, 0.242293, 0.189161])
}

flow_join_zipf = {
    'detect_skew': 0.0888254 / 4,
    'process_probe': np.max([0.24765, 0.244236, 0.235815, 0.273832])
}


# Plot
fig, axs = plt.subplots(2, 1, figsize=(16, 6))

categories = ['Hash Join', 'Flow-Join']

# no skew
no_skew_data = [
    hash_join_no_skew['process_probe'],
    flow_join_no_skew['detect_skew'] + flow_join_no_skew['process_probe']
]

# skew
skew_data = [
    hash_join_zipf['process_probe'],
    flow_join_zipf['detect_skew'] + flow_join_zipf['process_probe']
]

colors = ['#f0a207', '#399164']  # Colors for detect skew, process probe

# Plot no skew
axs[0].barh(categories, no_skew_data, color=[colors[1], colors[1]])
axs[0].barh(categories[1], flow_join_no_skew['detect_skew'], color=colors[0])

axs[0].set_title('No skew')
axs[0].set_xlim(0, 0.5)
axs[0].set_xlabel('Maximum Time (s)')
axs[0].legend(['process probe', 'detect skew'], loc='upper right')

# Plot skew
axs[1].barh(categories, skew_data, color=[colors[1], colors[1]])
axs[1].barh(categories[1], flow_join_zipf['detect_skew'], color=colors[0])

axs[1].set_title('Skew (Zipf 1.25)')
axs[1].set_xlim(0, 0.5)
axs[1].set_xlabel('Maximum Time (s)')
axs[1].legend(['process probe', 'detect skew'], loc='upper right')

plt.tight_layout()
plt.savefig('comparison_joins.jpg', format='jpg')
plt.show()
