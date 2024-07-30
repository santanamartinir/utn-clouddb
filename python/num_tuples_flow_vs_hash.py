import matplotlib.pyplot as plt
import numpy as np

# Assuming the provided data
data = np.array([
    [759388, 2077608],
    [742049, 1513831],
    [749005, 1893498],
    [755535, 2015165]
])

# Creating the bar plot
servers = ['1', '2', '3', '4']
no_skew = data[:, 0]
skew = data[:, 1]

x = np.arange(len(servers))  # the label locations
width = 0.35  # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(x - width/2, no_skew, width, label='flow-join', color='blue')
rects2 = ax.bar(x + width/2, skew, width, label='hash-join', color='lightgrey')

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('# Tuples')
ax.set_xlabel('server')
ax.set_xticks(x)
ax.set_xticklabels(servers)
ax.legend()
plt.title('Num. of sent tuples for hash- and flow-join')

# Saving the plot with high DPI for quality
plt.savefig('num_tuples_flow_vs_hash.jpeg', format='jpeg', dpi=300)
plt.show()