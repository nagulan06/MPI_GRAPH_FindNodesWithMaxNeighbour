#include <iostream>
#include <mpi.h>
#include <fstream>
#include <queue>
#include <utility>
#include <vector>

using namespace std;

//#define NODES 317080
#define NODES 317080L

int main()
{
    int rank, size, index;
    ifstream file;
    long nodes[NODES][NODES] = {0}, x, y;
    string useless;
    priority_queue<pair<long ,long> > pq;
    priority_queue<pair<long ,long> > pq_final;
    //initialize MPI
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    long split = NODES/size;
    //read file contents on the head node and create a graph with adjacency matrix
    if(rank == 0)
    {
        file.open("dblp-co-authors.txt");
//        file.open("test.txt");        
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
    MPI_Bcast(&nodes[0][0], NODES*NODES, MPI_LONG, 0, MPI_COMM_WORLD);

    //split the matrix to each node and let the each node find the author with maximimum co-authors
    //Then, each nodes add its found value to a vector
    //This vector is collected at the head node and the final list of authors with maximum co-authors is printed out.
    long k = 0;
    for(long i = rank*split; k<split; i++)
    {
      //  cout<<"node: "<<rank<<" range: "<<rank*split<<" to "<<rank*split + split-1<<endl;
        long count = 0;
        for(long j=0; j<NODES; j++)
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
            long k = 0;
            for(long i = NODES - 1; k < (NODES/size); i--)
            {
        //    cout<<"node: "<<rank<<" xtra range: "<<NODES-1<<" to "<<i<<endl;
                long count = 0;
                for(long j = 0; j<NODES; j++)
                {
                    if(nodes[i][j] == 1)
                        count++;
                }
                k++;
                pq.push(make_pair(count, i+1));
            }
        } 
    }
 //create a vector that contains all the authors with most number of co-authors (calculated by individual nodes) 
    vector<long> local_vec;
    pair<long, long> top = pq.top();
    long local_best = top.first;
    while(1 && !pq.empty())
    {
        local_vec.push_back(top.first);
        local_vec.push_back(top.second);
        pq.pop();
        top = pq.top();
        if(top.first != local_best)
            break;
    }
    for(long i=0; i<local_vec.size(); i++)
    {
        cout<<rank<<":  "<<local_vec[i]<<"  ";
    }
    cout<<endl;

    MPI_Barrier(MPI_COMM_WORLD);

    int *counts = new int[size];
    int nelements = (int)local_vec.size();
    // Each process tells the root how many elements it holds
    MPI_Gather(&nelements, 1, MPI_INT, counts, 1, MPI_INT, 0, MPI_COMM_WORLD);


    int *disps = new int[size];
   // Displacement for each node is calculated
    for (int i = 0; i < size; i++)
       disps[i] = (i > 0) ? (disps[i-1] + counts[i-1]) : 0; 
    

    long* final_vec = NULL;
    long total = disps[size-1] + counts[size-1];
    if(rank == 0)
    {
        final_vec = new long [total];
    }

   // Gatherv is used to gather variable length arrays (in this case, Vector)
    MPI_Gatherv(&local_vec[0], nelements, MPI_LONG, final_vec, counts, disps, MPI_LONG, 0, MPI_COMM_WORLD);
/*
    if(rank == 0)
    {   
        cout<<rank<<":  ";
        for(int i = 0; i < total; i++)
            cout<<final_vec[i]<<"  ";
    }
    cout<<endl;*/

    if(rank == 0)
    {
        for(int i=0; i<total; i=i+2)
            pq_final.push(make_pair(final_vec[i], final_vec[i+1]));

        pair<long, long> top_final = pq_final.top();
        
        int best = top_final.first;
        cout<<"The maximum number of co-authors is = "<<best<<endl;
        cout<<"And the list of authors with maximum co-authors are: "<<endl;
        while(1 && !pq_final.empty())
        {
            cout<<top_final.second<<" ";
            pq_final.pop();
            top_final = pq_final.top();
            if(top_final.first != best)
                break;
        }
        
    }

    MPI_Finalize();
    return 0;
}
