import sys
from pathlib import Path
import datetime
import os
import pandas as pd
import grpc
from concurrent import futures
import random

import master_pb2
import master_pb2_grpc

class Mapper(master_pb2_grpc.MasterServicer):
    def __init__(self, port, id):
        self.id = id
        self.port = port
        self.intermediate_file = f"intermediate_files_mapper_{self.id}/intermediate_mapper_{self.id}.txt"
        self.dump_file = f"mapper_dump/mapper_{self.id}.txt"
        if not os.path.exists(f'intermediate_files_mapper_{self.id}'):
            os.makedirs(f'intermediate_files_mapper_{self.id}')
        
    def write_to_dump(self, data):
        if not Path(self.dump_file).is_file():
            with open(self.dump_file, 'w') as f:
                f.write(f"Mapper {self.id} dump file\n")
        else:
            with open(self.dump_file, 'a') as f:
                f.write(f"{datetime.datetime.now()} - {data}\n")
                
    def GetInput(self, request, context):
        reducer_id = request.reducer_id
        self.write_to_dump(f"Reducer {reducer_id} requested input")
        if reducer_id - 1 not in self.partitions:
            return master_pb2.GetInputResponse(success=False, centroid_point_list = [])
        list = [master_pb2.Centroid_Point_Map(centroid_id = i[0], point = master_pb2.DataPoint(x = i[1][0], y = i[1][1])) for i in self.partitions[reducer_id - 1]]
        return master_pb2.GetInputResponse(success=True, centroid_point_list = list)
        
                
    def StartMapper(self, request, context):
        print("Mapper received request")
        self.write_to_dump("Mapper received request")

        self.num_reducers = request.num_reducers
        self.num_centroids = request.num_centroids
        self.input_file = request.input_file
        self.centroids = [(i.centroid_id, [i.point.x, i.point.y]) for i in request.centroids]

        self.centroids = [list(elem) for elem in self.centroids]

        for i in range(len(self.centroids)):
            self.centroids[i][1] = [round(x, 2) for x in self.centroids[i][1]]
            
        data_points = pd.read_csv(self.input_file, header=None).values.tolist()
        
        intermediate_output = self.KMeans_mapper(data_points, self.centroids)
        self.partitions = self.partition_data(intermediate_output, self.num_reducers)
        for partition_key, partition_value in self.partitions.items():
            with open(f'intermediate_files_mapper_{self.id}/intermediate_mapper_{partition_key + 1}.txt', 'w') as f:
                for item in partition_value:
                    f.write(f"{item[0]} {item[1]}\n")
        self.write_to_dump("Intermediate file written")
        
        if(random.random() < 0.5):
            self.write_to_dump("Mapper failed")
            return master_pb2.MapperResponse(success=False, intermediate_file="")
        return master_pb2.MapperResponse(success=True, intermediate_file=self.intermediate_file)

    def euclidean_distance(self, point1, point2):
        distance = 0
        for i in range(len(point1)):
            distance += (point1[i] - point2[i]) ** 2
        return distance ** 0.5
    
    def KMeans_mapper(self, data_points, centroids):
        intermediate_output = []
        for data_point in data_points:
            min_distance = float('inf')
            closest_centroid = None
            for centroid in centroids:
                distance = self.euclidean_distance(data_point, centroid[1])
                if distance < min_distance:
                    min_distance = distance
                    closest_centroid = centroid
            intermediate_output.append((closest_centroid[0], data_point))
        return intermediate_output
    
    # def partition_data(self, intermediate_output, num_reducers):
    #     partitions = {i: [] for i in range(num_reducers)}
    #     for item in intermediate_output:
    #         partitions[item[0] % num_reducers].append(item)
    #     return partitions
    
    def partition_data(self, data, num_reducers):
        partitions = {i: [] for i in range(num_reducers)}
        for item in data:
            partitions[item[0] % num_reducers].append(item)
        return partitions


def serve(port, id):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        master_pb2_grpc.add_MasterServicer_to_server(Mapper(port, id), server)
        server.add_insecure_port(f'[::]:{port}')
        print(f"Mapper {id} server started on port {port}")
        server.start()
        server.wait_for_termination()        


if __name__ == "__main__":
    id = int(sys.argv[1])
    port = int(sys.argv[2])
    serve(port, id)