import pickle

def pickle_appendable_unpack(path):
    try:
        with open(path, 'rb') as f:
            data = pickle.load(f)
            # data should be of type dict, so I shoudl return the dictionary data structure
            print("data type:" , type(data))
            return data
        
    except (EOFError, FileNotFoundError):
        return []

# main 
if __name__ == '__main__':
    # path is zatt_cluster/zatt.0.persist
    path = 'zatt_cluster/zatt.0.persist/state'
    print(pickle_appendable_unpack(path))
    