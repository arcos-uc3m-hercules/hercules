#ifndef UTILS_
#define UTILS_

#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <math.h> // for fabs in double comparition.
#include <stdbool.h> // for bool.


int get_hostname(char *buffer, size_t buffer_size);

/**
 * @brief Calculates the difference between two doubles in order to 
 * compare if they are almost equals.
 * @return true if the values are under the tolerance value, false in other case. 
 */
static const double epsilon = 1e-9; // A small tolerance value
bool double_are_equal(double a, double b);

#endif
