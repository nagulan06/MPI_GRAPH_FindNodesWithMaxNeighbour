#include <iostream>
#include <mpi.h>
#include <fstream>
#include <queue>
#include <utility>
#include <vector>

using namespace std;

//#define NODES 317080
#define NODES 5

int main()
{
    int rank, size, index;
    ifstream file;
    int nodes[NODES][NODES] = {0}, x, y;
    string useless;
    priority_queue<pair<int ,int> > pq;
    //initialize MPI
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    int split = NODES/size;
    //read file contents on the head node and create a graph with adjacency matrix
    if(rank == 0)
    {
//        file.open("dblp-co-authors.txt");
        file.open("test.txt");        
        if(file.fail())
            cerr<<"could not open file";

        for(int i=0; i<5; i++)
            getline(file, useless);

        while(!(file.eof()))
        {
            file>>x>>y;
            nodes[x-1][y-1] = 1;
            nodes[y-1][x-1] = 1;
        }
    }
    
    //send the adjacency matrix to all other nodes by broadcast.
    MPI_Bcast(&nodes[0][0], NODES*NODES, MPI_INT, 0, MPI_COMM_WORLD);

    //split the matrix to each node and let the each node find the author with maximimum co-authors
    //Then, each nodes add its found value to a vector
    //This vector is collected at the head node and the final list of authors with maximum co-authors is printed out.
    int k = 0;
    for(int i = rank*split; k<split; i++)
    {
      //  cout<<"node: "<<rank<<" range: "<<rank*split<<" to "<<rank*split + split-1<<endl;
        int count = 0;
        for(int j=0; j<NODES; j++)
        {
            if(nodes[i][j] == 1)
                count++;
        }
        pq.push(make_pair(count, i+1));
        k++;
    }

    if(NODES%size != 0)
    {
        if(rank == 0)
        {
            int k = 0;
            for(int i = NODES - 1; k < (NODES/size); i--)
            {
        //    cout<<"node: "<<rank<<" xtra range: "<<NODES-1<<" to "<<i<<endl;
                int count = 0;
                for(int j = 0; j<NODES; j++)
                {
                    if(nodes[i][j] == 1)
                        count++;
                }
                k++;
                pq.push(make_pair(count, i+1));
            }
        } 
    }
    
   // cout<<endl;
    vector<int> local_vec;
        
    pair<int, int> top = pq.top();
    int local_best = top.first;
    while(1)
    {
        local_vec.push(top.first);
        local_vec.push(top.second);
        pq.pop();
        top = pq.top();
        if(top.first ! = local_best)
            break;
    }
    
    cout<<rank<<": count =  "<<top.first <<"  node = "<<top.second<<endl;

    MPI_Finalize();
    return 0;
}


