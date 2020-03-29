#include <iostream>
#include <mpi.h>
#include <fstream>
#include <queue>
#include <utility>
#include <vector>
#include <algorithm>

using namespace std;

#define NODES 317080 

int main()
{
    int rank, size, index;
    ifstream file;
    int a, b;
    int list[2099732] = {0};
    vector<int> adjlist[317080];
    vector<int> number;
    vector<pair<int ,int> > plot;
    int  x, y;
    string useless;
    //initialize MPI
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    long split = NODES/size;
    //read file contents on the head node
    if(rank == 0)
    {
        file.open("dblp-co-authors.txt");
        if(file.fail())
            cerr<<"could not open file";

        for(int i=0; i<5; i++)
            getline(file, useless);
        
        int in = 0;
        while(file>>a)
        {   
            list[in] = a;
            in++;
        }
    }
 
    //send the file contents to all other nodes by broadcast.
    MPI_Bcast(&list[0], 2099732, MPI_INT, 0, MPI_COMM_WORLD);

    //create an adjacency list with the file data
    for(int i=0; i<2099732; i = i+2)
    //for(int i=0; i<16; i = i+2)
    {
        x = list[i];
        y = list[i+1];
        adjlist[x-1].push_back(y-1);
        adjlist[y-1].push_back(x-1);
    }
    
   
    //Work is split between the nodes to find the number of co-authors for each author (No.of co-authors = Length of the each list)
    //Then, it is pushed into a vector
    //Now, each node will have a vector list
    int k = 0;
    for(int i = rank*split; k<split; i++)
    {
        int length = adjlist[i].size();    //number of co-authors
        number.push_back(length);
        k++;
    }
    //if the number of process is not exactly divisible by the number of rows, there will be some leftover
    //The head node handles them.
    if(NODES%size != 0)
    {
        if(rank == 0)
        {
            long k = 0;
            for(long i = NODES - 1; k < (NODES/size); i--)
            {
        //    cout<<"node: "<<rank<<" xtra range: "<<NODES-1<<" to "<<i<<endl;
                int length = adjlist[i].size();
                number.push_back(length);
                k++;
            }
        } 
    }
   
    //each node has a vector list calculated. It is now gathered in the head node.

    MPI_Barrier(MPI_COMM_WORLD);

    int *counts = new int[size];
    int nelements = (int)number.size();
    // Each process tells the root how many elements it holds
    MPI_Gather(&nelements, 1, MPI_INT, counts, 1, MPI_INT, 0, MPI_COMM_WORLD);


    int *disps = new int[size];
   // Displacement for each node is calculated
    for (int i = 0; i < size; i++)
       disps[i] = (i > 0) ? (disps[i-1] + counts[i-1]) : 0; 
    

    int* final_arr = NULL;
    int total = disps[size-1] + counts[size-1];
    if(rank == 0)
    {
        final_arr = new int [total];
    }

   // Gatherv is used to gather variable length arrays (in this case, Vector)
    MPI_Gatherv(&number[0], nelements, MPI_INT, final_arr, counts, disps, MPI_INT, 0, MPI_COMM_WORLD);

    //finally the head calculates the number of authors with exactly 'd' co-authors and writes into an output file.
    if(rank == 0)
    {
        ofstream out;
        out.open("output_file.txt");
        out<<"Data to generate plot"<<endl;
        out<<"x"<<" "<<"y"<<endl;
        int count = 0, curr, prev;
        sort(final_arr, final_arr + total);
        prev = final_arr[0];

        for(int i=0; i<total; i++)
        {
            curr = final_arr[i];
            if(prev != curr)
            {
                out<<prev<<" "<<count<<endl;
               // plot.push_back(make_pair(prev, count));     //the pair has "the number of authors" published with exactly "d number of co-authors" 
                count = 0;
            }
            count++;
            prev = curr;
        }
        out<<prev<<" "<<count<<endl;
    }

    MPI_Finalize();
    return 0;
}
