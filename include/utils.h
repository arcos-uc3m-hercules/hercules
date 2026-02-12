#ifndef UTILS_
#define UTILS_

#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <math.h> // for fabs in double comparition.
#include <stdbool.h> // for bool.

// Definiciones de colores ANSI
#define ANSI_COLOR_RED     "\x1b[31m"
#define ANSI_COLOR_GREEN   "\x1b[32m"
#define ANSI_COLOR_YELLOW  "\x1b[33m"
#define ANSI_COLOR_BLUE    "\x1b[34m"
#define ANSI_COLOR_MAGENTA "\x1b[35m"
#define ANSI_COLOR_CYAN    "\x1b[36m"
#define ANSI_COLOR_RESET   "\x1b[0m"

int get_hostname(char *buffer, size_t buffer_size);

/**
 * @brief Calculates the difference between two doubles in order to 
 * compare if they are almost equals.
 * @return true if the values are under the tolerance value, false in other case. 
 */
static const double epsilon = 1e-9; // A small tolerance value
bool double_are_equal(double a, double b);

#endif
