#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <float.h>
#include <string.h>

#include <unistd.h>
#include "panjo-lib.h"

struct arginfo* ext_args = 0;


struct arginfo core_args[] = {
    { 'f', STRINGARG, &input_filename, "The input distance matrix filename in row major order." },
    { 'o', STRINGARG, &output_filename, "The output filename for the tree."},
    { 0, NULLARG, NULL, NULL },
};

void
dump_args (FILE* out, const struct arginfo* arg)
{
  for (; arg->c != 0; ++arg) {
    fprintf (out, "\t-%c: %s", arg->c, arg->desc);
    switch (arg->type) {
    case INTARG:
      fprintf (out, " (default: %d)\n", *(int*)(arg->val));
      break;
    case DOUBLEARG:
      fprintf (out, " (default: %lg)\n", *(double*)(arg->val));
      break;
    case STRINGARG:
      {
	char* carg = *(char**)(arg->val);
	fprintf (out, " (default: %s)\n", (carg? carg : "none"));
	break;
      }
    case NULLARG:
    default:
    fprintf (out, "\n");
      break;
    }
  }
}

void
dump_usage(FILE* out, const char *const pathname)
{
  const struct arginfo* arg;

  fprintf(out, "%s [-h|-?]",
	  pathname);
  if (ext_args)
    for (arg = ext_args; arg->c != 0; ++arg) {
      fprintf(out, " [-%c%s]", arg->c,
	      (INTARG == arg->type? " #" :
	       (DOUBLEARG == arg->type? " #.##" :
		(STRINGARG == arg->type? " <string>" : ""))));
    }
  for (arg = core_args; arg->c != 0; ++arg) {
    fprintf(out, " [-%c%s]", arg->c,
	    (INTARG == arg->type? " #" :
	     (DOUBLEARG == arg->type? " #.##" :
	      (STRINGARG == arg->type? " <string>" : ""))));
  }
  fprintf (out, "\n");

  fprintf(stderr, "\t-h or -?: Display this.\n");
  if (ext_args)
    dump_args (out, ext_args);
  dump_args (out, core_args);
}

static int
parse_arg (int c, struct arginfo* arg)
{
  extern char *optarg;

  for (; arg->c != 0; ++arg) {
    if (c == arg->c) {
      switch (arg->type) {
      case NULLARG:
	{
	  *(int*)(arg->val) = 1;
	  return 1;
	}
	break;
      case INTARG:
	{
	  int tmp = atoi(optarg);
	  if (tmp <= 0) return 0; /* Pretend it wasn't found. */
	  *(int*)(arg->val) = tmp;
	  return 1;
	}
	break;
      case DOUBLEARG:
	{
	  double tmp = strtod(optarg, NULL);
	  if (tmp <= 0) return 0; /* Pretend it wasn't found. */
	  *(double*)(arg->val) = tmp;
	  return 1;
	}
	break;
      case STRINGARG:
	{
	  char* cptr = *(char**)(arg->val);
	  size_t len;
	  if (cptr)
	    /* Might be static... Hopefully free will silently fail. */
	    free(cptr);
	  len = strlen(optarg);
	  if (len > STRINGARG_MAXLEN) len = STRINGARG_MAXLEN;
	  cptr = malloc (len+1);
	  strncpy (cptr, optarg, len+1);
	  *(char**)(arg->val) = cptr;
	  return 1;
	}
	break;
      default:
	return 0; /*NEVERREACHED*/
      }
    }
  }
  return 0;
}

void
get_options(int argc, char **argv)
{
  int c;

  extern char *optarg;
  extern int optind, opterr, optopt;

  char* optstring;
  char* cptr;
  int len_optstring = 2;
  const struct arginfo* arg;

  for (arg = core_args; arg->c != 0; ++arg) {
    ++len_optstring;
    if (NULLARG != arg->type) ++len_optstring;
  }
  if (ext_args)
    for (arg = ext_args; arg->c != 0; ++arg) {
      ++len_optstring;
      if (NULLARG != arg->type) ++len_optstring;
    }

  optstring = malloc ((len_optstring+1) * sizeof(char));
  optstring[0] = 'h';
  optstring[1] = '?';
  optstring[len_optstring] = '\000';

  cptr = optstring + 2;
  for (arg = core_args; arg->c != 0; ++arg) {
    *cptr++ = arg->c;
    if (NULLARG != arg->type) *cptr++ = ':';
  }
  if (ext_args)
    for (arg = ext_args; arg->c != 0; ++arg) {
      *cptr++ = arg->c;
      if (NULLARG != arg->type) *cptr++ = ':';
    }

  while (-1 != (c = getopt(argc, argv, optstring))) {
    if ('h' == c || '?' == c) {
      dump_usage(stdout, argv[0]);
      exit(0);
    }
    if (parse_arg(c, core_args)) continue;
    if (ext_args && parse_arg(c, ext_args)) continue;
    dump_usage(stderr, argv[0]);
    exit(-1);
  }
}

void
initialize_timer (struct Timer * t)
{
  t->clock_holder.tv_sec = 0;
  t->clock_holder.tv_usec = 0;
  t->duration.tv_sec = 0;
  t->duration.tv_usec = 0;
}

void
start_timer(struct Timer * t)
{
  gettimeofday (&t->clock_holder, NULL);
}

void
stop_timer(struct Timer * t)
{
  struct timeval end_tv;
  gettimeofday (&end_tv, NULL);
  t->duration.tv_sec += (end_tv.tv_sec - t->clock_holder.tv_sec);
  t->duration.tv_usec += (end_tv.tv_usec - t->clock_holder.tv_usec);
}

double
timer_duration(const struct Timer t)
{
  return t.duration.tv_sec + 1.0e-6 * (double)t.duration.tv_usec;
}
