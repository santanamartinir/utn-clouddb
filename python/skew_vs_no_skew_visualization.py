import matplotlib.pyplot as plt
import numpy as np

# Assuming the provided data
data = np.array([
    [2499619, 1685098],
    [2500751, 3947112],
    [2497478, 2427998],
    [2502152, 1939792]
])

# Creating the bar plot
servers = ['1', '2', '3', '4']
no_skew = data[:, 0]
skew = data[:, 1]

x = np.arange(len(servers))  # the label locations
width = 0.35  # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(x - width/2, no_skew, width, label='no skew', color='lightgrey')
rects2 = ax.bar(x + width/2, skew, width, label='skew', color='blue')

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('# Tuples')
ax.set_xlabel('server')
ax.set_xticks(x)
ax.set_xticklabels(servers)
ax.legend()
plt.title('Num. of tuples for hash-join')

# Saving the plot with high DPI for quality
plt.savefig('skew_vs_no_skew.jpeg', format='jpeg', dpi=300)
plt.show()