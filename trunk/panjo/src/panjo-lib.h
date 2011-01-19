/**
 * This header was based on CS-267 fish-lib header.
 */

#if !defined(PANJO_H_)
#define PANJO_H_

#include "config.h"

/* For struct Timer routines. */
#include <sys/time.h>
#include <time.h>

#if defined(HAVE_ISNAN) || defined(HAVE_FMAX)
#include <math.h>
#endif

#define STRINGARG_MAXLEN 1024
typedef enum { NULLARG, INTARG, DOUBLEARG, STRINGARG } arg_type_t;

struct arginfo {
  char c;
  arg_type_t type;
  void* val;
  const char* desc;
};

struct proc_attr {
    int start;
    int end;
    int rank;
    int N;
    double* matrix;
};
typedef struct proc_attr proc_attr_t;

struct node {
    int seq_index;
    int is_leaf; 
    double left_distance;
    double right_distance; 
    struct node* left; 
    struct node* right; 

};
typedef struct node node_t; 



int owns(proc_attr_t, int, int);
void store(proc_attr_t, int, int, double);
double get(proc_attr_t, int, int);

extern struct arginfo* ext_args;
extern char* input_filename;
extern char* output_filename;


/* --- Useful functions -------------------------------------------- */

/* Process the command line options */
void get_options(int, char **);

void dump_usage(FILE* out, const char *const pathname);

/* A simple wrapper around the clock() timer */
struct Timer {
  struct timeval clock_holder;
  struct timeval duration;
};

void initialize_timer (struct Timer* t);
void start_timer (struct Timer* t);
void stop_timer (struct Timer* t);
double timer_duration(const struct Timer t);



#endif /* PANJO_H_ */
