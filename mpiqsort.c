#include "header.h"
#define DEBUG_PRINT_ENABLED 1
#if DEBUG_PRINT_ENABLED
#define DEBUG printf
#else
#define DEBUG(format, args...) ((void)0)
#endif
#define XOR ^

int compare (const void * a, const void * b)
{
  return ( *(int*)a - *(int*)b );
}
/* Returns index of last element less than equal to key */
int binary_search(int *a, int low, int high, int key) {
    int orig_high = high,mid,midVal,last_low;
    for(int i=low;i<high;i++){
        assert(a[i]<=a[i+1]);
    }
	while (low <= high) {
		mid = low + (high - low) / 2;
		midVal = a[mid];
		if (midVal < key)
			low = mid + 1;
		else if (midVal > key)
			high = mid - 1;
		else 
            break;
	}
    if(a[mid]==key){
        /* We have an exact match. Account for duplicates */
        while(mid<=orig_high && a[mid]==key)
            mid++;
        last_low = mid-1;
    }
    else if(a[mid]>key)
        last_low = mid-1;
    else
        last_low = mid;
    assert(last_low<=orig_high && a[last_low]<=key);
    if(last_low+1<=orig_high)
        assert(a[last_low+1]>key);
	return last_low;
}
/* Returns a new list after merging the two input lists */
int* merge_lists(int* a, int sizea, int* b, int sizeb){
	int* c = (int*)malloc((sizea+sizeb)*sizeof(int));
	int i=0, j=0, k=0;
	while(i < sizea && j < sizeb){
		if(a[i] < b[j]){
			c[k++] = a[i++];
		}else{
			c[k++] = b[j++];
		}
	}
	memcpy(c+k, a+i, sizeof(int)*(sizea-i));
	memcpy(c+k, b+j, sizeof(int)*(sizeb-j));
	return c;
}
int* mpiqsort(int* input, int globalNumElements, int* dataLengthPtr, MPI_Comm comm, int commRank, int commSize) {
    int *mylist = input;
    int n_elems = *dataLengthPtr;
    int pivot;
    int myid    = commRank, new_rank, partner, root;
    int d = (int) (log(commSize)/log(2));
    int ranks[commSize];
    MPI_Group orig_group, my_group, new_group;
    MPI_Comm new_comm;
    /* Know my group initially */ 
    MPI_Comm_group(MPI_COMM_WORLD, &orig_group);
    my_group = orig_group;
    for(int j=0;j<commSize;j++){
        ranks[j]=j;
    }
    /* To begin, sort locally*/
    //printf("Process %d (pid:%d) attempting to sort locally... \n",myid,getpid());
    qsort(mylist,n_elems,sizeof(int),compare);
    for(int i=d-1;i>=0;i--){
        if(myid == 0){
            /* I'm the leader. I get to select pivot. */
            pivot = *(mylist+(n_elems/2));
        }
        MPI_Bcast((void *)&pivot,1,MPI_INT,0,comm);
        partner = myid XOR (1<<i);
        int split_index = binary_search(mylist,0,n_elems-1,pivot);
        //printf("Bsearch done. Split index is %d\n",split_index);
        int num_low = split_index+1;
        int num_high = n_elems - num_low;
        int num_retained;
        if(partner<myid){
            /* Send lower sublist and retain higher*/
            MPI_Send((void *)&num_low,1,MPI_INT,partner,i,comm);
            printf("[%d] Id(%d) : Sending %d elems to %d... \n",getpid(),myid,num_low,partner);
            MPI_Send((void *)mylist,num_low,MPI_INT,partner,i,comm);
            mylist+=num_low;
            //printf("%d\n",*mylist);
            num_retained = num_high;
        } else if(partner > myid) {
            /* Send higher sublist and retain lower*/
            MPI_Send((void *)&num_high,1,MPI_INT,partner,i,comm);
            printf("[%d] Id(%d) : Sending %d elems to %d... \n",getpid(),myid,num_high,partner);
            MPI_Send((void *)(mylist+num_low),num_high,MPI_INT,partner,i,comm);
            num_retained = num_low;
            //printf("[%d] Id(%d) : Pivot is %d. Sent %d elems to %d. Retained %d elems\n",getpid(),myid,pivot,num_high,partner,num_retained);
        }
        int num_recvd = 0;
        MPI_Status *status = (MPI_Status *)malloc(sizeof(MPI_Status));
        MPI_Recv((void *)&num_recvd,1,MPI_INT,partner,i,comm,status);
        int *recv_list = (int *)malloc(num_recvd*sizeof(int));
        MPI_Recv((void *)recv_list,num_recvd,MPI_INT,partner,i,comm,status);
        mylist = merge_lists(mylist,num_retained,recv_list,num_recvd);
        //printf("[%d] Id(%d) : Merged lists\n",getpid(),myid,pivot,partner);
        /* n_elems for next iter */
        n_elems = num_retained + num_recvd;
        //printf("[%d] Id(%d) : My new num elems is %d\n",getpid(),myid,n_elems);
        /* We are done with one level. Split the group for next level. */
        MPI_Group_incl(my_group,
                       commSize/2,
                       ranks+((myid>=commSize/2)?(commSize/2):0),
                       &new_group);
        // Note: In the next iter, all processes will again point
        // to the same rank array, but will only deal with first
        // half of the elements.
        /* Constrict my community for next iter */
        MPI_Comm_create(MPI_COMM_WORLD, new_group, &new_comm);
        /* Get my new rank in the new community */
        MPI_Group_rank(new_group,&new_rank);
        comm = new_comm;
        my_group = new_group;
        myid = new_rank;
        commSize = commSize/2;
    }
    *dataLengthPtr = n_elems;
    return mylist;
}
