from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

if rank == 0:
    data = {'key1' : [7, 2.72, 2+3j],
            'key2' : ( 'abc', 'xyz')}
else:
    data = None
data = comm.bcast(data, root=0)

if __name__=="__main__":
    # parser = argparse.ArgumentParser()
    # parser.add_argument("shpfile", help="Shapefile location")
    # parser.add_argument('-t', '--tilesize', help="Size of tile in meters", required=True)
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    print(f"Rank: {rank}")

    if rank == 0:
        data = {'key1' : [7, 2.72, 2+3j],
                'key2' : ( 'abc', 'xyz')}
    else:
        data = None
    data = comm.bcast(data, root=0)

