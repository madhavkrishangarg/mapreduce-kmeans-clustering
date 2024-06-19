import matplotlib.pyplot as plt

#plot points in input.txt
def plot_points(input_file_path):
    with open(input_file_path, 'r') as f:
        lines = f.readlines()
    data_points = []
    for line in lines:
        data_points.append(list(map(float, line.strip().split(','))))
    plt.scatter(*zip(*data_points))


#plot centroids
def plot_centroids(centroids_file_path):
    with open(centroids_file_path, 'r') as f:
        lines = f.readlines()
    centroids = []
    for line in lines:
        centroids.append(list(map(float, line.strip().split(' ')))[1:])
    print(centroids)
    plt.scatter(*zip(*centroids), color='red')
    
    
plot_points('input.txt')
plot_centroids('output/output_3.txt')
    
plt.show()