import matplotlib.pyplot as plt

def read_data(filename):
    """Reads unsigned integer values from a file and returns a list of these values."""
    data = []
    with open(filename, 'r') as file:
        for line in file:
            data.append(int(line.strip()))
    return data

def plot_histogram_percentage(data, output_file, title):
    """Plots a histogram from a list of unsigned integer values with percentages on the y-axis."""
    plt.hist(data, bins=range(min(data), max(data) + 2), edgecolor='black', density=True)
    plt.xlabel('Value')
    plt.ylabel('%')
    plt.title(title)
    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
    plt.savefig(output_file, format='jpeg', dpi=300)


def main():
    filename = 'zipf_10_1p25_12_100000.txt'
    data = read_data(filename)
    plot_histogram_percentage(data, "Zipf_histogram.jpeg", "Zipf, alpha=1.25")

if __name__ == '__main__':
    main()
