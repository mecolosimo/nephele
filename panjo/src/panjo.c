#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/unistd.h>   // for gethostname
#include <math.h>    // for INFINITY
#ifdef __USE_MPI__
#include "mpi.h"
#endif

#include "panjo-lib.h"

#define VERBOSE 0
#define REALLY_VERBOSE 0

#define __BUILD_TREE__ 1

#define start_mpi_timer(t) do { *(t) -= MPI_Wtime(); } while (0)
#define stop_mpi_timer(t) do { *(t) += MPI_Wtime(); } while (0)

// mec for linux boxes with gcc
#if defined INFINITY
static const float BIG_NUMBER = INFINITY;
#else
static const float BIG_NUMBER = __builtin_inf();
#endif

char* input_filename = 0;
char* output_filename = 0;
char hostname[128];
int my_rank = 0;

void tree_print (FILE* out, node_t* node) {

    if (node != NULL) {
	if (node->is_leaf) {
	  fprintf(out,"%d", node->seq_index);
	}
	else {
	  fprintf(out,"(");
	  tree_print(out, node->left);
	  fprintf(out,",");
	  tree_print(out, node->right);
	  fprintf(out,")");
	}
    }
}

int ij_to_n(int i, int j, int N) {
    return (j*N + i);
}

int owns(proc_attr_t proc, int i, int j) {
    return (((j >= proc.start) && (j < proc.end)) ? 1 : 0);
}

int who_owns(int j, int P, int N) {
    int i;
    int per_proc = (int) (N / P);

    for (i = 0; i < P; i++) { 
	if ((j >= (i*per_proc)) && (j < ((i+1)*per_proc)))
	    return i;
    }
}

void store(proc_attr_t proc, int i, int j, double d) {
    proc.matrix[ij_to_n(i, j - proc.start, proc.N)] = d;
}

double get(proc_attr_t proc, int i, int j) {
    if (REALLY_VERBOSE)
	printf("proc: %d, getting i: %d, j: %d \n", proc.rank, i, j);
    return (proc.matrix[ij_to_n(i,j-proc.start, proc.N)]);
}

int get_dist_matrix_size() {
  printf("Panjo[%s:rank-%d]: Getting distance matrix size for '%s'\n", hostname, my_rank, input_filename);

    char number[30];
    unsigned int asize = 0;

    FILE* fp = fopen(input_filename, "r");
    if (fp == NULL) {
	printf("cannot open filename: %s for reading.\n", input_filename);
	return -1;
    }

    while (fscanf(fp, "%s", number) == 1) {
	asize++;
    }
    fclose(fp);
    
    return asize;
}


int read_dist_matrix(proc_attr_t this_proc) 
{
    if (VERBOSE) 
      printf("Panjo[%d:rank-%d]: reading distance matrix: %s\n", hostname, this_proc.rank, input_filename);
    
    double d = 0;
    FILE* fp = NULL;
    int i = 0;
    int j = 0;

    /** this stores the number in a character, because i can't get it 
	to work using %f. **/
    char number[30];
    
    if (VERBOSE) 
	printf("will read a matrix of size:%d\n", this_proc.N);
    
    fp = fopen(input_filename, "r");
    if (fp == NULL) {
	printf("cannot open filename: %s for reading.\n", input_filename);
	return -1;
    }

    /** column major layout in the file. */
    for (j = 0; j < this_proc.N; j++) {
	for (i = 0; i < this_proc.N; i++) {
	    if (fscanf(fp, "%s", number) == 1) {
		d = strtod(number, NULL);
		
		if(owns(this_proc, i, j)) {
		    store(this_proc, i, j, d);
		}
	    }
	}
    }
    fclose(fp);

    if (REALLY_VERBOSE) {
	printf("<initial distance matrix>\n");
	for (j = this_proc.start; j < this_proc.end; j++) {
	    for (i = 0; i < this_proc.N; i++) {
		printf("%g ", get(this_proc, i, j));
	    }
	    printf("\n");
	}
    }
    return 0;
}


main(int argc, char* argv[]) {
  gethostname(hostname, sizeof hostname);
  unsigned N = 0, K = 0, TS = 0;
  FILE* out_fp = NULL;
    int P = 1;
    int i,j;
    double minD, tmpD;
    proc_attr_t this_proc;
    double g_minD = 0;
    double NJ_tree_score = 0;
    int n_messages = 0;

    /** used to dennote the owner of data. */
    int w_proc = -1;
    
    /** the timers used. **/
    double mpi_timer = 0;
    struct Timer total_timer;

#ifdef __BUILD_TREE__
    /** only on the root . **/
    node_t** tree;
#endif

#ifdef __USE_MPI__
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &P);
#endif

    /** parse the command line options. **/
    get_options(argc, argv);

    if (!(input_filename)) {
      printf("Panjo[%s:rank-%d]: Must specify input filename.\n", hostname, my_rank);
      dump_usage(stdout, argv[0]);
      /** here we need to signal to the rest of the guys to die! **/
      exit(1);
    }

    if (output_filename ) {
      out_fp = fopen(output_filename, "w");
      if (out_fp == NULL) {
	printf("Panjo[%s:rank-%d]: Cannot open output file '%s' for writing.\n", hostname, my_rank, output_filename);
	/** here we need to signal to the rest of the guys to die! **/
	exit(1);
      } else if (my_rank == 0) {
	printf("Panjo[%s:rank-%d]: Tree will be written to '%s'.\n", hostname, my_rank, output_filename);
      }
    } else {
      printf("Panjo[%s:rank-%d]: Warning using standard output to write tree to!\n", hostname, my_rank);
      out_fp = stdout;
    }
    
    /** get the total size. **/
    if (my_rank == 0) { 
	if ((TS = get_dist_matrix_size()) < 0) {
	  printf("Panjo[%s:rank-%d]: Problem getting size of distance matrix.\n", hostname, my_rank);
	  /** here we need to signal to the rest of the guys to die! **/
	  exit(1);
	}
    }
    
#ifdef __USE_MPI__
    /** Broadcast the size of the matrix to all of the processors. **/
    MPI_Bcast(&TS, 1, MPI_INT, 0, MPI_COMM_WORLD);

    /** send P messages of size 1. **/
    /*    n_messages += P;    */
#endif
    
    N = (int) sqrt(TS);
    K = (int) (N / P);
    
    if (N % P != 0) {
      if (my_rank == 0) {
      /** find the largest divisor of N working down from the number of processors given. */
      int ld;
      printf("trying to find largest divisor for %d starting at %d\n", N, P);
      for (ld = P; ld >= 1; ld--) {
	if (N % ld == 0) {
	  break;
	}
      }
      printf("Panjo[%s:rank-%d]: Matrix size (%d) and number of processors (%d) not evenly divisible! Largest divisor lower than given processors is %d\n", hostname, my_rank, N, P, ld);
      } else {
printf("Panjo[%s:rank-%d]: Matrix size (%d) and number of processors (%d) not evenly divisible! Check main (rank 0) for possible largest divisor lower.\n", hostname, my_rank, N, P);
      }
 //printf("Panjo does not support %d processors for a matrix of size %d. Please select ve a processor which supports division by N (%d).\n", hostname, N);
	exit(1);
	/** 
	 * Not completed see MPI_Allgather below!!!
	 * Give the extra to the first processor. 
	 */
	if (my_rank == 0) {
	  printf("Panjo[%s:rank-%d]: Warning matrix size (%d) and number of processors (%d) not evenly divisible.We are getting the remainder (%d) \n", hostname, my_rank, N, P, (N % P));
	    K += (N % P);
	}
    }

    this_proc.start = (K*my_rank) + ((my_rank == 0) ? 0 : (N % P));
    this_proc.end = this_proc.start + K;
    this_proc.rank = my_rank;
    this_proc.matrix = (double*) malloc(sizeof(double)*K*N);
    this_proc.N = N;

    if (this_proc.rank == 0) {
      printf("Panjo[%s:main]: Beginning Neighbor-Join Algorithm.\n", hostname);
	printf("\t N = %d\n", N);
	printf("\t P = %d\n", P);
	printf("\t K = %d\n", K);
    }
    if (VERBOSE) {
      printf("Panjo[%s:rank-%d]:Getting width of: %d with start: %d and end: %d\n",
	     hostname, my_rank, K,  this_proc.start, this_proc.end);
    }

    /** allocate space. **/
    short int* valid = (short int*) malloc(sizeof(short int)*N);
    double* R = (double*) malloc(sizeof(double) * N);
    double* minimums = (double*) malloc(sizeof(double) * P);
    double D_min = 0.0;
    int* minij = (int*) malloc(sizeof(int) * 2);
    double* exchange_buffer = (double*) malloc(sizeof(double) * N);
    int clusters = N;

#ifdef __BUILD_TREE__
    /** on the root proc we make the tree. **/
    if (this_proc.rank == 0) {
	tree = (node_t **) malloc(sizeof(node_t *) * N);

	for (i = 0; i < N; i++) {
	    node_t* leaf = (node_t*) malloc(sizeof(node_t));
	    
	    leaf->left = NULL;
	    leaf->right = NULL;
	    leaf->left_distance = 0;
	    leaf->right_distance = 0;
	    leaf->seq_index = i + 1;
	    leaf->is_leaf = 1;
	    tree[i] = leaf;

	    if (VERBOSE) 
		printf("new node: (%g, %g)\n", tree[i]->left_distance, tree[i]->right_distance);
	}
    }
#endif
  
    for (i = 0; i < N; i++) {
	valid[i] = 1;
	R[i] = 0;
	exchange_buffer[i] = 0;
    }
    for (i = 0; i < P; i++) {
	minimums[i] = BIG_NUMBER;
    }
    
    /** read the appropriate piece into your memory. **/
    if (read_dist_matrix(this_proc) < 0) {
      printf("Panjo[%d:rank-%d]:Problem reading distance matrix.", hostname, my_rank);
    }

    initialize_timer(&total_timer);
    start_timer(&total_timer);

    /** The main loop. **/
    while (clusters > 2) {
	if (REALLY_VERBOSE) {
	    for (i = 0; i < N; i++) {
		printf("%d ", valid[i]);
	    }
	    printf("\n");
	}

	/** Column Sums **/
	for (j = this_proc.start; j < this_proc.end; j++) {
	    if (valid[j]) {
		R[j] = 0.0;
		for (i = 0; i < N; i++) {
		    if (valid[i] && i != j) {
			R[j] += get(this_proc, i, j);
		    }
		}
		R[j] /= ((double) clusters - 2.0);
	    }
	    if (REALLY_VERBOSE)
		printf("R[j]=%g", R[j]);
	}
	if (REALLY_VERBOSE) 
	    printf("\n");
    
#ifdef __USE_MPI__
	start_mpi_timer(&mpi_timer);
	/**
	   Here is a problem - if the processor count is not divisible by the number of columns
	   then we have to do something a bit more complicated here, which for now I am going to 
	   avoid.
	*/
	MPI_Allgather(&R[this_proc.start], (this_proc.end - this_proc.start), MPI_DOUBLE_PRECISION,
		      R,  (this_proc.end - this_proc.start), MPI_DOUBLE_PRECISION, MPI_COMM_WORLD);
	stop_mpi_timer (&mpi_timer);

	/** something like P^2 messages of size (N/P)*double. */
#endif

	/** Find a minimum D_min **/
	minij[0] = minij[1] = -1;
	minD = BIG_NUMBER;
    
	for (j = this_proc.start; j < this_proc.end; j++) {
	    if (valid[j]) {
		for (i = j + 1; i < N; i++) {
		    if (valid[i]) {
			tmpD = get(this_proc, i, j) - (R[i] + R[j]);
			if (tmpD < minD) {
			    minij[0] = i;
			    minij[1] = j;
			    minD = tmpD;
			}
		    }
		}
	    }
	}

	if (REALLY_VERBOSE)
	    printf("proc: %d - (mini: %d, minj: %d) = %g\n", this_proc.rank, minij[0], minij[1], minD);
    
	/** save the minimum frome each processor. **/
	minimums[this_proc.rank] = minD;

#ifdef __USE_MPI__
	start_mpi_timer(&mpi_timer);
	MPI_Allgather(&minimums[this_proc.rank], 1, MPI_DOUBLE_PRECISION,
		      minimums, 1, MPI_DOUBLE_PRECISION, MPI_COMM_WORLD);
	stop_mpi_timer(&mpi_timer);

	/** P^2 size 1 **/
#endif

	g_minD = BIG_NUMBER;
	w_proc = -1;
	for (i = 0; i < P; i++) {
	    if (minimums[i] < g_minD) {
		w_proc = i;
		g_minD = minimums[i];
	    }
	}

	if (REALLY_VERBOSE) {
	    printf("proc: %d - (mini: %d, minj: %d) = %g\n", this_proc.rank, minij[0], 
		   minij[1], g_minD);
	}

	/** we need to get the minimum values from the original distance matrix. **/
	g_minD = (this_proc.rank == w_proc) ? get(this_proc, minij[0], minij[1]) : 0;
    
#ifdef __USE_MPI__    
	start_mpi_timer(&mpi_timer);
	MPI_Bcast(minij, 2, MPI_INT, w_proc, MPI_COMM_WORLD);
	MPI_Bcast(&g_minD, 1, MPI_DOUBLE, w_proc, MPI_COMM_WORLD);
	stop_mpi_timer(&mpi_timer);	

	/** 2P 2, 1 **/
#endif

	if (REALLY_VERBOSE) 
	    printf("proc: %d - (mini: %d, minj: %d) = %g\n", this_proc.rank, minij[0], minij[1], g_minD);
	
	/** set the i'th one to be no longer valid - this happens separately on each proc.**/
	valid[minij[0]] = 0;
    
	/** this is where we calculate the distances to the new node u* **/
	for (j = this_proc.start; j < this_proc.end; j++) {
	    if (valid[j]) {
		double tmp = .5*(get(this_proc, minij[0], j) + get(this_proc, minij[1], j) - g_minD);

		/** save it in our own space. **/
		store(this_proc, minij[1], j, tmp);
	    
		/** fill the exchange buffer. **/
		exchange_buffer[j] = tmp;
	    }
	    else {
		/** this should make the transpose look nice, but altogether uneccessary. */
		exchange_buffer[j] = get(this_proc, minij[1], j);
	    }
	}

	int to_proc = who_owns(minij[1], P, N);

#ifdef __USE_MPI__
	start_mpi_timer(&mpi_timer);
	MPI_Gather(&exchange_buffer[this_proc.start], (this_proc.end - this_proc.start), 
		   MPI_DOUBLE_PRECISION, exchange_buffer, (this_proc.end - this_proc.start),
		   MPI_DOUBLE_PRECISION, to_proc, MPI_COMM_WORLD);
	stop_mpi_timer(&mpi_timer);
#endif

	/** if you are the owner process then you need to write the the stuff from the 
	    exchange_buffer to the appropriate column. **/
	if (to_proc == this_proc.rank) {
	    for (i = 0; i < N; i++) {
		if (valid[i]) {
		    store(this_proc, i, minij[1], exchange_buffer[i]);
		}
	    }
	}
	
	if (this_proc.rank == 0) {
	    /** 
		we are going to build the tree on the root processor. 
	    **/
	    double dik = .5*(g_minD + R[minij[0]] - R[minij[1]]);
	    double djk = g_minD - dik;
	    
	    if (VERBOSE) {
		printf("grouping (%d,%d) with distances (%g, %g) R[i] = %g, R[j] = %g, clusters=%d\n", 
		       minij[0], minij[1], dik, djk, R[minij[0]], R[minij[1]], clusters);
	    }

#ifdef __BUILD_TREE__
	    if (VERBOSE) 
		printf("joining nodes: %d, %d \n", minij[0], minij[1]);

	    node_t* internal_node = (node_t*) malloc(sizeof(node_t));
	    internal_node->left = tree[minij[0]];
	    internal_node->right = tree[minij[1]];
	    internal_node->left_distance = dik;
	    internal_node->right_distance = djk;
	    internal_node->is_leaf = 0;
	    
	    /** we invalidate i - so we shove it back in j. **/
	    tree[minij[1]] = internal_node;
#endif
	}

	/** keep track of the tree score. **/
	NJ_tree_score += g_minD;

#ifdef __USE_MPI__
	start_mpi_timer(&mpi_timer);
	MPI_Barrier(MPI_COMM_WORLD);
	stop_mpi_timer(&mpi_timer);
#endif
	if (REALLY_VERBOSE) {
	    for (j = this_proc.start; j < this_proc.end; j++) {
		printf("p.%d:", this_proc.rank);
		for (i = 0; i < N; i++) {
		    printf("%g ", get(this_proc, i, j));
		}
		printf("\n");
	    }
	}
	clusters--;
    }
    

    /** here we have to join the last two clusters. **/
    int li = -1;
    int lj = -1;

    for (i = 0; i < N; i++) {
	if (valid[i]) {
	    if (li < 0) 
		li = i;
	    else 
		lj = i;
	}
    }

    /** now the owner sends it to the root. **/
    w_proc = who_owns(lj, P, N);
    if (this_proc.rank == w_proc) {
	g_minD = get(this_proc, li, lj);
	NJ_tree_score += g_minD;
    }
    
#ifdef __USE_MPI__
    start_mpi_timer(&mpi_timer);
    MPI_Bcast(&g_minD, 1, MPI_DOUBLE, w_proc, MPI_COMM_WORLD);
    MPI_Bcast(&NJ_tree_score, 1, MPI_DOUBLE, w_proc, MPI_COMM_WORLD);
    stop_mpi_timer(&mpi_timer);
#endif

    stop_timer(&total_timer);

    if (this_proc.rank == 0) {
	/** get final distances. **/
	double dik = .5*(g_minD + R[li] - R[lj]);
	double djk = g_minD - dik;

	printf("TreeScore: %g\n", NJ_tree_score);
	printf("MPI time: %g\n", mpi_timer);
	printf("Total time: %g\n", timer_duration(total_timer));
	printf("Benchmark Output: %d, %d, %g, %g\n", N, P, timer_duration(total_timer), mpi_timer);
	
#ifdef __BUILD_TREE__
	node_t* root = (node_t*) malloc(sizeof(node_t));
	
	/** finish up the tree. **/
	root->left = tree[li];
	root->right = tree[lj];
	root->left_distance = dik;
	root->right_distance = djk;
	root->is_leaf = 0;
	
	/** print the tree. **/
	printf("Neighbor-Joining Tree:\n\t");


	tree_print(out_fp, root);
	printf("\n");
#endif
    }
    
#ifdef __USE_MPI__
    MPI_Finalize();
#endif
    
    return 0;
}
