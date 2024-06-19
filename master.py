from concurrent import futures
import logging
import grpc
import os
import time
import subprocess
import shutil
from multiprocessing.pool import ThreadPool
import random

import master_pb2
import master_pb2_grpc

class Master(master_pb2_grpc.MasterServicer):
    
    def __init__(self, input_file_path, n_mappers, n_reducers, n_centroids, n_iterations):
        self.input_file_path = input_file_path
        self.n_mappers = n_mappers
        self.n_reducers = n_reducers
        self.n_centroids = n_centroids
        self.n_iterations = n_iterations
        self.output_folder = "output"
        self.mappers = {}
        self.reducers = {}
        self.initial_centroids = self.initialize_centroids()
        self.initial_centroids = [[i, self.initial_centroids[i]] for i in range(len(self.initial_centroids))]
        print("Initial centroids")
        print(self.initial_centroids)
        self.input_split_path = 'input_split/input_mapper_{}.txt'
        
    def input_split(self):
        with open(self.input_file_path, 'r') as f:
            lines = f.readlines()
            n_lines = len(lines)
            lines_per_mapper = n_lines // self.n_mappers  
            extra_lines = n_lines % self.n_mappers  
            start_idx = 0  
            for i in range(1, self.n_mappers+1):
                end_idx = start_idx + lines_per_mapper
                if extra_lines > 0:
                    end_idx += 1  
                    extra_lines -= 1
                with open(self.input_split_path.format(i), 'w') as f_out:
                    f_out.writelines(lines[start_idx:end_idx])
                start_idx = end_idx  

    def spawn_mappers(self):
        self.mappers = {}
        base_port = 5000
        for i in range(1, self.n_mappers + 1):
            port = base_port + i
            p = subprocess.Popen(['python3', 'mapper.py', str(i), str(port)])
            # print(f"Mapper {i} started on port {port}")
            print(f"Mapper PID: {p.pid}")
            self.mappers[i] = p
        with open('mappers.txt', 'a') as f:
            for i in self.mappers:
                f.write(f"{i} {self.mappers[i].pid}\n")
            
    def spawn_reducers(self):
        self.reducers = {}
        base_port = 6000
        for i in range(1, self.n_reducers + 1):
            port = base_port + i
            p = subprocess.Popen(['python3', 'reducer.py', str(i), str(port)])
            # print(f"Reducer {i} started on port {port}")
            # time.sleep(0.1)
            print(f"Reducer PID: {p.pid}")
            self.reducers[i] = p
        with open('reducers.txt', 'a') as f:
            for i in self.reducers:
                f.write(f"{i} {self.reducers[i].pid}\n")
            
    def kill_mappers(self):
        for i in self.mappers:
            self.mappers[i].kill()
            print(f"Mapper {i} killed")
        with open('mappers.txt', 'r') as f:
            lines = f.readlines()
        for line in lines:
            mapper_id, pid = map(int, line.strip().split())
            #if pid exists
            if os.path.exists(f'/proc/{pid}'):
                os.kill(pid, 9)
                print(f"Mapper {mapper_id} killed")
    
    def kill_reducers(self):
        for i in self.reducers:
            self.reducers[i].kill()
            print(f"Reducer {i} killed")
        with open('reducers.txt', 'r') as f:
            lines = f.readlines()
        for line in lines:
            reducer_id, pid = map(int, line.strip().split())
            if os.path.exists(f'/proc/{pid}'):
                os.kill(pid, 9)
                print(f"Reducer {reducer_id} killed")
            
    def initialize_centroids(self):
        with open(self.input_file_path, 'r') as f:
            lines = f.readlines()
        data_points = []
        for line in lines:
            data_points.append(list(map(float, line.strip().split(','))))
        centroids = []
        for i in range(self.n_centroids):
            centroids.append(random.choice(data_points))
        return centroids
    
    def run_kmeans(self):
        for i in range(self.n_iterations):
            print(f"\n\nIteration {i+1}")
            self.RPC_to_all_mappers()
            #RPC to reducers only after all mappers have finished
            self.RPC_to_all_reducers()
            
            with open(f'{self.output_folder}/output_{i+1}.txt', 'w') as f:
                for centroid in self.initial_centroids:
                    f.write(f"{centroid[0]} {centroid[1][0]} {centroid[1][1]}\n")
                    
            if self.check_convergence(self.initial_centroids, i):
                print("\n\nConverged\n\n")
                
                break
            
        print(f"\n\nFinal centroids: {self.initial_centroids}\n\n")
        self.kill_mappers()
        self.kill_reducers()
                    
    def check_convergence(self, centroids1, iteration):
        if iteration == 0:
            return False
        with open(f'{self.output_folder}/output_{iteration}.txt', 'r') as f:
            lines = f.readlines()
        centroids2 = []
        for line in lines:
            centroid_id, x, y = map(float, line.strip().split())
            centroids2.append([centroid_id, [x, y]])
        for i in range(len(centroids1)):
            if centroids1[i][1] != centroids2[i][1]:
                return False
        return True
        
            
    def RPC_to_all_mappers(self):
        with ThreadPool(self.n_mappers) as pool:
            pool.starmap(self.map_function, [(5000 + i, i) for i in self.mappers])
                
    def RPC_to_all_reducers(self):
        with ThreadPool(self.n_reducers) as pool:
            pool.starmap(self.reduce_function, [(6000 + i, i) for i in self.reducers])
            
    
    def map_function(self, port, id):
        try:
            with grpc.insecure_channel(f'localhost:{port}') as channel:
                    stub = master_pb2_grpc.MasterStub(channel)
                    list_of_datapoints = [master_pb2.Centroid_Point_Map(centroid_id=i[0], point=master_pb2.DataPoint(x=i[1][0], y=i[1][1])) for i in self.initial_centroids]
                    response = stub.StartMapper(master_pb2.MapperRequest(input_file=self.input_split_path.format(id), num_reducers=self.n_reducers, num_centroids=self.n_centroids, centroids=list_of_datapoints))
                    
                    if response.success:
                        print(f"Mapper {id} finished, intermediate file: {response.intermediate_file}")
                    else:
                        print(f"Mapper {id} failed, trying again")
                        self.map_function(port, id)
                        
        except Exception as e:
            print("Error in map function, trying again", e)
            self.map_function(port, id)
            

    def reduce_function(self, port, id):
        try:
            with grpc.insecure_channel(f'localhost:{port}') as channel:
                    stub = master_pb2_grpc.MasterStub(channel)
                    response = stub.StartReducer(master_pb2.ReducerRequest(num_reducers=self.n_reducers, num_mappers=self.n_mappers, mapper_base_port=5000))
                    
                    if response.success:
                        new_centroids = [[i.centroid_id, [i.point.x, i.point.y]] for i in response.new_centroids]
                        for i in range(len(new_centroids)):
                            new_centroids[i][1] = [round(j, 2) for j in new_centroids[i][1]]

                        for i in range(len(new_centroids)):
                            for j in range(len(self.initial_centroids)):
                                if new_centroids[i][0] == self.initial_centroids[j][0]:
                                    self.initial_centroids[j][1] = new_centroids[i][1]
                        print(f"Reducer {id} finished", new_centroids)
                    else:
                        print(f"Reducer {id} failed, trying again")
                        self.reduce_function(port, id)
                        
        except Exception as e:
            print("Error in reduce function, trying again", e)
            self.reduce_function(port, id)
        
if __name__ == '__main__':
    number_of_mappers = int(input("Enter number of mappers: "))
    number_of_reducers = int(input("Enter number of reducers: "))
    number_of_centroids = int(input("Enter number of centroids: "))
    number_of_iterations = int(input("Enter number of iterations: "))
    master = Master('input.txt', number_of_mappers, number_of_reducers, number_of_centroids, number_of_iterations)
    master.input_split()
    master.kill_mappers()
    master.kill_reducers()
    master.spawn_mappers()
    master.spawn_reducers()
    time.sleep(1.5)
    try:
        master.run_kmeans()
    except Exception as e:
        print(e)