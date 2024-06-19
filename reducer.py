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

class Reducer(master_pb2_grpc.MasterServicer):
    def __init__(self, port, id):
        self.id = id
        self.port = port
        self.intermediate_file = f"reducers/r_{self.id}.txt"
        self.dump_file = f"reducer_dump/dump_reducer_{self.id}.txt"
        # print(f"Reducer {self.id} initialized")
        
    def write_to_dump(self, data):
        if not Path(self.dump_file).is_file():
            with open(self.dump_file, 'w') as f:
                f.write(f"Reducer {self.id} dump file\n")
        else:
            with open(self.dump_file, 'a') as f:
                f.write(f"{datetime.datetime.now()} - {data}\n")    
        
    def compute_updated_centroids(self, sorted_data_points):
        centroids = []
        for centroid_id in sorted_data_points:
            data_points = sorted_data_points[centroid_id]
            x = sum([i[0] for i in data_points])/len(data_points)
            y = sum([i[1] for i in data_points])/len(data_points)
            centroids.append([centroid_id, [x, y]])
        return centroids
    
    def shuffle_and_sort(self, data_points):
        grouped_data_points = {}
        for data_point in data_points:
            centroid_id = data_point[0]
            if centroid_id not in grouped_data_points:
                grouped_data_points[centroid_id] = []
            grouped_data_points[centroid_id].append(data_point[1])
        return grouped_data_points
    
    def StartReducer(self, request, context):
        print("Reducer received request")
        self.write_to_dump("Reducer received request")
        self.num_reducers = request.num_reducers
        self.num_mappers = request.num_mappers
        self.base_port = request.mapper_base_port
        self.data_points = []
        for i in range(1, self.num_mappers+1):
            try:
                with grpc.insecure_channel(f'localhost:{self.base_port+i}') as channel:
                    stub = master_pb2_grpc.MasterStub(channel)
                    # print("Self ID: ", self.id)
                    response = stub.GetInput(master_pb2.GetInputRequest(reducer_id=self.id))
                    if response.success:
                        # print(f"Reducer {self.id} received input from Mapper {i}")
                        temp_data_points = [[i.centroid_id, [i.point.x, i.point.y]] for i in response.centroid_point_list]
                        #roundoff to 2 decimal places
                        for _ in range(len(temp_data_points)):
                            temp_data_points[_][1] = [round(j, 2) for j in temp_data_points[_][1]]
                        
                        self.data_points.extend(temp_data_points)
                        print(f"Reducer {self.id} received data points from Mapper {i}")
                        self.write_to_dump(f"Reducer {self.id} received data points from Mapper {i}")
            except Exception as e:
                print("Error in reducer call to mapper", e)
                
        try:        
            sorted_data_points = self.shuffle_and_sort(self.data_points)
            print(f"Reducer {self.id} completed shuffle and sort")
            self.write_to_dump(f"Reducer {self.id} completed shuffle and sort")
            
            new_centroids = self.compute_updated_centroids(sorted_data_points)

            for i in range(len(new_centroids)):
                new_centroids[i][1] = [round(j, 2) for j in new_centroids[i][1]]
            print(f"Reducer {self.id} computed updated centroids")
            self.write_to_dump(f"Reducer {self.id} computed updated centroids {new_centroids}")
            # print(f"Reducer {self.id} new centroids: ", new_centroids)

            with open(self.intermediate_file, 'w') as f:
                for centroid in new_centroids:
                    f.write(f"{centroid[0]}: {centroid[1][0]} {centroid[1][1]}\n")

            list = [master_pb2.Centroid_Point_Map(centroid_id = i[0], point = master_pb2.DataPoint(x = i[1][0], y = i[1][1])) for i in new_centroids]
            # print(list)
            
            if(random.random() < 0.5):
                self.write_to_dump("Reducer failed")
                return master_pb2.ReducerResponse(success=False)
            return master_pb2.ReducerResponse(success=True, new_centroids = list)
        
        except Exception as e:
            print("Error in reducer", e)
            return master_pb2.ReducerResponse(success=False)
    
def serve(port, id):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        master_pb2_grpc.add_MasterServicer_to_server(Reducer(port, id), server)
        server.add_insecure_port(f'[::]:{port}')
        print(f"Reducer {id} started on port {port}")
        server.start()
        server.wait_for_termination()
        
if __name__ == '__main__':
    id = int(sys.argv[1])
    port = int(sys.argv[2])
    serve(port, id)