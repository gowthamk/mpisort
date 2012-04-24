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
    int orig_high = high,mid,midVal;
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
    if(low<=high){
        /* We have an exact match */
        while(mid<=orig_high && a[mid]==key)
            mid++;
        mid--;
    }
    assert(mid<=orig_high && a[mid]<=key);
    if(mid+1<=orig_high)
        assert(a[mid+1]>key);
	return mid;
}
/* Returns a new list after merging the two input lists */
int *merge_lists(int *list1,int size1,int *list2,int size2)
{
    int *newlst = (int *)malloc((size1+size2)*sizeof(int));
    int i,j,k;
    for(i=0,j=0,k=0;i<size1 && j<size2;){
        if(list1[i]<=list2[j]){
            newlst[k++]=list1[i++];
        }
        else {
            newlst[k++]=list2[j++];
        }
    }
    if(i<size1){
        assert(k+i==size1+size2);
        memcpy((newlist+k),list1,(size1-i)*sizeof(int));
    }
    if(j<size2){
        assert(k+j==size1+size2);
        memcpy((newlist+k),list2,(size2-j)*sizeof(int));
    }
    return newlst;
}
int* mpiqsort(int* input, int globalNumElements, int* dataLengthPtr, MPI_Comm comm, int commRank, int commSize) {
    int *mylist = input;
    int n_elems = *dataLengthPtr;
    int myid    = commRank;
    int pivot,root;
    int d = (int) log(commSize)/log(2);
    int ranks[commSize];
    MPI_Group orig_group, my_group, new_group;
    MPI_Comm new_comm;
    /* Know my group initially */ 
    MPI_Comm_group(MPI_COMM_WORLD, &orig_group);
    my_group = orig_group;
    for(int j=0;j<commSize;j++){
        ranks[i]=i;
    }
    /* To begin, sort locally*/
    qsort(mylist,n_elems,sizeof(int),compare);
    for(i=d-1;i>=0;i--){
        if(myid == 0){
            /* I'm the leader. I get to select pivot. */
            pivot = *(mylist+(n_elems/2));
        }
        MPI_Bcast((void *)pivot,1,MPI_INT,0,comm);
        DEBUG("Id(%d) : Pivot is %d\n",myid,pivot);
        partner = myid XOR pow(2,i);
        int split_index = binary_search(mylist,0,n_elems,pivot);
        int num_low = split_index+1;
        int num_high = n_elems - num_low;
        if(partner<myid){
            /* Send lower sublist and retain higher*/
            MPI_Send((void *)num_low,1,MPI_INT,partner,i,comm);
            MPI_Send((void *)mylist,1,MPI_INT,partner,i,comm);
            mylist+=num_low;
            num_retained = num_high;
        } else if(partner > myid) {
            /* Send higher sublist and retain lower*/
            MPI_Send((void *)num_high,1,MPI_INT,partner,i,comm);
            MPI_Send((void *)(mylist+num_low),1,MPI_INT,partner,i,comm);
            num_retained = num_low;
        }
        int num_recvd = 0;
        MPI_Status *status = (MPI_Status *)malloc(sizeof(MPI_Status));
        MPI_Recv((void *)&num_recvd,1,MPI_INT,partner,i,comm,status);
        int *recv_list = (int *)malloc(num_recvd*sizeof(int));
        MPI_Recv((void *)recv_list,num_recvd,MPI_INT,partner,i,comm,status);
        mylist = merge_lists(mylist,num_retained,recv_list,num_recvd);
        /* n_elems for next iter */
        n_elems = num_retained + num_recvd;
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
        comm = new_comm;
        my_group = new_group;
        comSize = comSize/2;
    }
    /* Concatenate all lists and return */
    return mylist;
}
